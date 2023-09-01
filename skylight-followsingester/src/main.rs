mod firehose;

use clap::Parser;
use futures::{SinkExt, StreamExt};
use sqlx::Connection;
use tracing::Instrument;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "postgres:///skygraph")]
    dsn: String,

    #[arg(long, default_value = "wss://bsky.social")]
    firehose_host: String,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let args = Args::parse();

    let mut conn = sqlx::postgres::PgConnection::connect(&args.dsn).await?;

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
                let msg = if let Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(msg))) = msg? {
                    msg
                } else {
                    continue;
                };
                process_message(&mut conn, &msg)
                    .instrument(tracing::info_span!("process_message"))
                    .await?;
            }
        }
    }
}

#[derive(Debug, serde::Deserialize)]
struct FirehoseHeader {
    #[serde(rename = "op")]
    operation: i8,

    #[serde(rename = "t")]
    r#type: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct FirehoseError {
    error: String,
    message: Option<String>,
}

async fn get_did_for_id(
    executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
    did: &str,
) -> Result<i32, sqlx::Error> {
    Ok(sqlx::query!(
        r#"
        WITH e AS (
            INSERT INTO follows.dids (did)
            VALUES ($1)
            ON CONFLICT (did) DO
            NOTHING
            RETURNING id
        )
        SELECT id AS "id!"
        FROM e
        UNION
        SELECT id AS "id!"
        FROM follows.dids
        WHERE did = $1
        "#,
        did
    )
    .fetch_one(executor)
    .await?
    .id)
}

async fn process_message(
    conn: &mut sqlx::postgres::PgConnection,
    message: &[u8],
) -> Result<(), anyhow::Error> {
    let mut cursor = std::io::Cursor::new(message);
    let frame: FirehoseHeader = ciborium::from_reader(&mut cursor)?;
    if frame.operation == -1 {
        let error: FirehoseError = ciborium::from_reader(&mut cursor)?;
        return Err(anyhow::format_err!(
            "{}: {}",
            error.error,
            error.message.unwrap_or_else(|| "".to_string())
        ));
    }

    if frame.operation != 1 {
        return Err(anyhow::format_err!(
            "expected frame.op = 1, got {}",
            frame.operation
        ));
    }

    let mut tx = conn.begin().await?;
    let seq = match frame.r#type.unwrap_or_else(|| "".to_string()).as_str() {
        "#info" => {
            let info: firehose::Info = ciborium::from_reader(&mut cursor)?;
            tracing::info!(name = info.name, message = info.message);
            return Ok(());
        }
        "#commit" => {
            let commit: firehose::Commit = ciborium::from_reader(&mut cursor)?;
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

                        let actor_id = get_did_for_id(&mut *tx, &commit.repo).await?;
                        let subject_id = get_did_for_id(&mut *tx, &record.subject).await?;
                        sqlx::query!(
                            r#"
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
                            rkey = rkey
                        )
                    }
                    "delete" => {
                        sqlx::query!(
                            r#"
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
                            rkey = rkey
                        );
                    }
                    _ => {
                        continue;
                    }
                }
            }
            commit.seq
        }
        "#tombstone" => {
            let tombstone: firehose::Tombstone = ciborium::from_reader(&mut cursor)?;
            sqlx::query!(
                r#"
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
            tombstone.seq
        }
        "#handle" => {
            let handle: firehose::Handle = ciborium::from_reader(&mut cursor)?;
            handle.seq
        }
        "#migrate" => {
            let migrate: firehose::Migrate = ciborium::from_reader(&mut cursor)?;
            migrate.seq
        }
        _ => {
            return Ok(());
        }
    };
    sqlx::query!(
        r#"
        INSERT INTO follows.cursor (cursor)
        VALUES ($1)
        ON CONFLICT ((0)) DO
        UPDATE SET cursor = excluded.cursor
        "#,
        seq
    )
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}
