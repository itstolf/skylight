use std::str::FromStr;

use clap::Parser;
use futures::TryStreamExt;
use sqlx::Connection;
use tracing::Instrument;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "postgres:///skygraph")]
    dsn: String,

    #[arg(long, default_value = "https://bsky.social")]
    pds_host: String,

    #[arg(long, default_value_t = 8)]
    num_workers: usize,

    #[arg(long)]
    only_crawl_queued_repos: bool,
}

type RateLimiter = governor::RateLimiter<
    governor::state::NotKeyed,
    governor::state::InMemoryState,
    governor::clock::QuantaClock,
    governor::middleware::NoOpMiddleware<governor::clock::QuantaInstant>,
>;

struct DidIdAssginer {
    conn: sqlx::postgres::PgConnection,
}

impl DidIdAssginer {
    async fn assign(&mut self, did: &str) -> Result<i32, sqlx::Error> {
        Ok(sqlx::query!(
            r#"
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

async fn worker_main(
    pds_host: String,
    client: reqwest::Client,
    rl: std::sync::Arc<RateLimiter>,
    queued_notify: std::sync::Arc<tokio::sync::Notify>,
    mut conn: sqlx::PgConnection,
    mut did_id_assigner: DidIdAssginer,
) -> Result<(), anyhow::Error> {
    loop {
        loop {
            let mut tx = conn.begin().await?;
            let did = if let Some(did) = sqlx::query!(
                r#"
                DELETE FROM followscrawler.pending
                WHERE
                    did = (
                        SELECT did
                        FROM followscrawler.pending
                        FOR UPDATE
                        SKIP LOCKED
                        LIMIT 1
                    )
                RETURNING did
                "#
            )
            .fetch_optional(&mut *tx)
            .await?
            .map(|r| r.did)
            {
                did
            } else {
                queued_notify.notified().await;
                tracing::info!("wakeup");
                continue;
            };

            if let Err(err) = {
                let rl = &rl;
                let pds_host = &pds_host;
                let client = &client;
                let did_id_assigner = &mut did_id_assigner;
                let did = did.clone();
                (move || async move {
                    rl.until_ready().await;
                    let repo = tokio::time::timeout(
                        std::time::Duration::from_secs(30 * 60),
                        atproto_repo::load(
                            &mut tokio::time::timeout(
                                std::time::Duration::from_secs(10 * 60),
                                client
                                    .get(format!(
                                        "{}/xrpc/com.atproto.sync.getCheckout?did={}",
                                        pds_host, did
                                    ))
                                    .send(),
                            )
                            .await??
                            .error_for_status()?
                            .bytes_stream()
                            .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
                            .into_async_read(),
                            true,
                        ),
                    )
                    .await??;

                    let mut records = vec![];
                    for (key, cid) in repo.key_and_cids() {
                        let key = String::from_utf8_lossy(key);
                        let (collection, rkey) = match key.splitn(2, '/').collect::<Vec<_>>()[..] {
                            [collection, rkey] => (collection, rkey),
                            _ => {
                                continue;
                            }
                        };

                        if collection != "app.bsky.graph.follow" {
                            continue;
                        }

                        let block = if let Some(block) = repo.get_by_cid(cid) {
                            block
                        } else {
                            continue;
                        };

                        #[derive(serde::Deserialize, Debug, Clone, PartialEq, Eq)]
                        #[serde(rename_all = "camelCase")]
                        struct Record {
                            created_at: String,
                            subject: String,
                        }

                        let record: Record =
                            match ciborium::from_reader(std::io::Cursor::new(block)) {
                                Ok(record) => record,
                                Err(e) => {
                                    tracing::error!(
                                        error = format!("ciborium::from_reader: {e:?}")
                                    );
                                    continue;
                                }
                            };
                        records.push((rkey.to_string(), record));
                    }

                    let n = records.len();
                    for (rkey, record) in records {
                        let actor_id = did_id_assigner.assign(&did).await?;
                        let subject_id = did_id_assigner.assign(&record.subject).await?;
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
                    }
                    tx.commit().await?;
                    tracing::info!(action = "repo", did = did, n = n);
                    Ok::<_, anyhow::Error>(())
                })()
                .await
            } {
                let why = format!("{:?}", err);
                tracing::error!(did, why);
                let mut tx = conn.begin().await?;
                sqlx::query!(
                    r#"
                    INSERT INTO followscrawler.errors (did, why)
                    VALUES ($1, $2)
                    "#,
                    did,
                    why
                )
                .execute(&mut *tx)
                .await?;
                tx.commit().await?;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let args = Args::parse();

    let conn_options = sqlx::postgres::PgConnectOptions::from_str(&args.dsn)?;
    let mut conn = sqlx::postgres::PgConnection::connect_with(&conn_options).await?;

    let rl = std::sync::Arc::new(governor::RateLimiter::direct(governor::Quota::per_second(
        std::num::NonZeroU32::new(3000 / (5 * 60)).unwrap(),
    )));

    let queued_notify = std::sync::Arc::new(tokio::sync::Notify::new());

    let client = reqwest::Client::new();

    let workers = (0..args.num_workers)
        .map(|i| {
            tokio::spawn({
                let conn_options = conn_options.clone();
                let pds_host = args.pds_host.clone();
                let client = client.clone();
                let rl = std::sync::Arc::clone(&rl);
                let queued_notify = std::sync::Arc::clone(&queued_notify);
                async move {
                    let conn = sqlx::postgres::PgConnection::connect_with(&conn_options).await?;
                    let did_id_assigner = DidIdAssginer {
                        conn: sqlx::postgres::PgConnection::connect_with(&conn_options).await?,
                    };
                    worker_main(pds_host, client, rl, queued_notify, conn, did_id_assigner)
                        .instrument(tracing::info_span!("worker", i))
                        .await
                }
            })
        })
        .collect::<Vec<_>>();

    if !args.only_crawl_queued_repos {
        let mut cursor = sqlx::query!("SELECT cursor FROM followscrawler.cursor")
            .fetch_optional(&mut conn)
            .await?
            .map(|v| v.cursor);

        if cursor != Some("".to_string()) {
            loop {
                let mut url = format!(
                    "{}/xrpc/com.atproto.sync.listRepos?limit=1000",
                    args.pds_host
                );
                if let Some(cursor) = cursor.as_ref() {
                    url.push_str(&format!("&cursor={}", cursor));
                }

                #[derive(serde::Deserialize, Debug, Clone, PartialEq, Eq)]
                #[serde(rename_all = "camelCase")]
                struct Output {
                    cursor: Option<String>,
                    repos: Vec<Repo>,
                }

                #[derive(serde::Deserialize, Debug, Clone, PartialEq, Eq)]
                #[serde(rename_all = "camelCase")]
                struct Repo {
                    did: String,
                    head: String,
                }

                rl.until_ready().await;
                let output: Output = serde_json::from_slice(
                    &client
                        .get(url)
                        .send()
                        .await?
                        .error_for_status()?
                        .bytes()
                        .await?,
                )?;

                let mut tx = conn.begin().await?;
                for repo in output.repos {
                    sqlx::query!(
                        r#"
                        INSERT INTO followscrawler.pending (did)
                        VALUES ($1)
                        ON CONFLICT DO
                        NOTHING
                        "#,
                        repo.did
                    )
                    .execute(&mut *tx)
                    .await?;
                }

                let c = output.cursor.unwrap_or_else(|| "".to_string());
                let done = c.is_empty();
                sqlx::query!(
                    r#"
                    INSERT INTO followscrawler.cursor (cursor)
                    VALUES ($1)
                    ON CONFLICT ((0)) DO
                    UPDATE SET cursor = excluded.cursor
                    "#,
                    c
                )
                .execute(&mut *tx)
                .await?;
                cursor = Some(c);
                tx.commit().await?;
                queued_notify.notify_waiters();

                if done {
                    break;
                }
            }
        }
    }

    futures::future::join_all(workers)
        .await
        .into_iter()
        .flatten()
        .collect::<Result<_, _>>()?;
    Ok(())
}
