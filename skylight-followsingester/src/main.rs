mod firehose;

use std::str::FromStr;

use clap::Parser;
use futures::{SinkExt, StreamExt};
use sqlx::Connection;
use tracing::Instrument;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "postgres:///skylight")]
    dsn: String,

    #[arg(long, default_value = "wss://bsky.social")]
    firehose_host: String,

    #[arg(long, default_value = "127.0.0.1:9000")]
    prometheus_listen: std::net::SocketAddr,
}

struct DidIdAssginer {
    conn: sqlx::postgres::PgConnection,
}

impl DidIdAssginer {
    async fn assign(&mut self, did: &str) -> Result<i32, sqlx::Error> {
        Ok(sqlx::query!(
            r#"--sql
            INSERT INTO follows.dids (did)
            VALUES ($1)
            ON CONFLICT (did) DO
            UPDATE SET did = excluded.did
            RETURNING id
            "#,
            did
        )
        .fetch_one(&mut self.conn)
        .await?
        .id)
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let args = Args::parse();

    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(args.prometheus_listen)
        .install()?;

    let conn_options = sqlx::postgres::PgConnectOptions::from_str(&args.dsn)?;
    let mut conn = sqlx::postgres::PgConnection::connect_with(&conn_options).await?;
    let mut did_id_assigner = DidIdAssginer {
        conn: sqlx::postgres::PgConnection::connect_with(&conn_options).await?,
    };

    let mut url = format!(
        "{}/xrpc/com.atproto.sync.subscribeRepos",
        args.firehose_host
    );
    if let Some(cursor) = sqlx::query!("SELECT cursor FROM follows.cursor")
        .fetch_optional(&mut conn)
        .await?
        .map(|v| v.cursor)
    {
        tracing::info!(cursor = cursor);
        url.push_str(&format!("?cursor={cursor}"));
    } else {
        tracing::info!("no cursor");
    }

    let (stream, _) = tokio_tungstenite::connect_async(url).await?;
    let (mut tx, mut rx) = stream.split();

    loop {
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                tokio::time::timeout(
                    std::time::Duration::from_secs(10),
                    tx.send(tokio_tungstenite::tungstenite::Message::Ping(vec![]))
                ).await??;
            }

            msg = tokio::time::timeout(std::time::Duration::from_secs(60), rx.next()) => {
                let msg = if let Some(msg) = msg? {
                    msg
                } else {
                    break;
                };

                let msg = if let tokio_tungstenite::tungstenite::Message::Binary(msg) = msg? {
                    msg
                } else {
                    continue;
                };

                process_message(&mut conn, &mut did_id_assigner, &msg)
                    .instrument(tracing::info_span!("process_message"))
                    .await?;
            }
        }
    }

    Ok(())
}

async fn process_message(
    conn: &mut sqlx::postgres::PgConnection,
    did_id_assigner: &mut DidIdAssginer,
    message: &[u8],
) -> Result<(), anyhow::Error> {
    let mut tx = conn.begin().await?;
    let (seq, time) = match firehose::Message::parse(message)? {
        firehose::Message::Info(info) => {
            tracing::info!(name = info.name, message = info.message);
            return Ok(());
        }
        firehose::Message::Commit(commit) => {
            for op in commit.ops {
                let (collection, rkey) = match op.path.splitn(2, '/').collect::<Vec<_>>()[..] {
                    [collection, rkey] => (collection, rkey),
                    _ => {
                        continue;
                    }
                };

                if collection != "app.bsky.graph.follow" {
                    continue;
                }

                let items = match rs_car::car_read_all(&mut &commit.blocks[..], true).await {
                    Ok((parsed, _)) => parsed
                        .into_iter()
                        .collect::<std::collections::HashMap<_, _>>(),
                    Err(e) => {
                        tracing::error!(
                            path = op.path,
                            error = format!("rs_car::car_read_all: {e:?}")
                        );
                        continue;
                    }
                };

                match op.action.as_str() {
                    "create" => {
                        let item = if let Some(item) = op.cid.and_then(|cid| items.get(&cid.into()))
                        {
                            item
                        } else {
                            continue;
                        };

                        #[derive(serde::Deserialize, Debug, Clone, PartialEq, Eq)]
                        #[serde(rename_all = "camelCase")]
                        struct Record {
                            created_at: String,
                            subject: String,
                        }

                        let record: Record = match ciborium::from_reader(std::io::Cursor::new(item))
                        {
                            Ok(record) => record,
                            Err(e) => {
                                tracing::error!(
                                    path = op.path,
                                    error = format!("ciborium::from_reader: {e:?}")
                                );
                                continue;
                            }
                        };

                        let actor_id = did_id_assigner.assign(&commit.repo).await?;
                        let subject_id = did_id_assigner.assign(&record.subject).await?;
                        sqlx::query!(
                            r#"--sql
                            INSERT INTO follows.edges (actor_id, rkey, subject_id)
                            VALUES ($1, $2, $3)
                            ON CONFLICT DO NOTHING
                            "#,
                            actor_id,
                            rkey,
                            subject_id
                        )
                        .execute(&mut *tx)
                        .await?;

                        tracing::info!(
                            action = "create follow",
                            seq = commit.seq,
                            actor_did = commit.repo,
                            subject_did = record.subject,
                            rkey = rkey,
                        )
                    }
                    "delete" => {
                        sqlx::query!(
                            r#"--sql
                            WITH ids AS (
                                SELECT id
                                FROM follows.dids
                                WHERE did = $1
                            )
                            DELETE FROM follows.edges
                            WHERE
                                actor_id IN (SELECT id FROM ids) AND
                                rkey = $2
                            "#,
                            commit.repo,
                            rkey,
                        )
                        .execute(&mut *tx)
                        .await?;

                        tracing::info!(
                            action = "delete follow",
                            seq = commit.seq,
                            actor_did = commit.repo,
                            rkey = rkey,
                        );
                    }
                    _ => {
                        continue;
                    }
                }
            }
            (commit.seq, commit.time)
        }
        firehose::Message::Tombstone(tombstone) => {
            sqlx::query!(
                r#"--sql
                WITH ids AS (
                    SELECT id
                    FROM follows.dids
                    WHERE did = $1
                )
                DELETE FROM follows.edges
                WHERE
                    actor_id IN (SELECT id FROM ids) OR
                    subject_id IN (SELECT id FROM ids)
                "#,
                tombstone.did
            )
            .execute(&mut *tx)
            .await?;
            (tombstone.seq, tombstone.time)
        }
        firehose::Message::Handle(handle) => (handle.seq, handle.time),
        firehose::Message::Migrate(migrate) => (migrate.seq, migrate.time),
    };
    sqlx::query!(
        r#"--sql
        INSERT INTO follows.cursor (cursor)
        VALUES ($1)
        ON CONFLICT ((0)) DO
        UPDATE SET cursor = excluded.cursor
        "#,
        seq
    )
    .execute(&mut *tx)
    .await?;

    let now = time::OffsetDateTime::now_utc();
    metrics::histogram!(
        "skylight_followsingester.ingest_delay",
        (now - time).as_seconds_f64()
    );

    tx.commit().await?;
    Ok(())
}
