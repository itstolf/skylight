use clap::Parser;
use futures::TryStreamExt;
use tracing::Instrument;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    db_path: std::path::PathBuf,

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

async fn worker_main(
    pds_host: String,
    client: reqwest::Client,
    rl: std::sync::Arc<RateLimiter>,
    queued_notify: std::sync::Arc<tokio::sync::Notify>,
    env: heed::Env,
    schema: skylight_followsdb::Schema,
    queued_db: heed::Database<heed::types::Str, heed::types::Unit>,
    pending_db: heed::Database<heed::types::Str, heed::types::Unit>,
    errored_db: heed::Database<heed::types::Str, heed::types::Str>,
) -> Result<(), anyhow::Error> {
    loop {
        queued_notify.notified().await;
        tracing::info!("wakeup");

        loop {
            let did = {
                let mut tx = env.write_txn()?;
                let did = if let Some((did, _)) = queued_db.first(&tx)? {
                    did.to_string()
                } else {
                    continue;
                };
                pending_db.put(&mut tx, &did, &())?;
                queued_db.delete(&mut tx, &did)?;
                tx.commit()?;
                did
            };

            if let Err(err) = {
                let env = env.clone();
                let rl = std::sync::Arc::clone(&rl);
                let pds_host = pds_host.clone();
                let client = client.clone();
                let schema = schema.clone();
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
                    let mut tx = env.write_txn()?;
                    for (rkey, record) in records {
                        // Crash if we can't write to followsdb.
                        skylight_followsdb::writer::add_follow(
                            &schema,
                            &mut tx,
                            &rkey,
                            &did,
                            &record.subject,
                        )
                        .expect("skylight_followsdb::writer::add_follow");
                    }
                    pending_db.delete(&mut tx, &did)?;
                    tx.commit()?;
                    tracing::info!(action = "repo", did = did, n = n);
                    Ok::<_, anyhow::Error>(())
                })()
                .await
            } {
                let mut tx = env.write_txn()?;
                errored_db.put(&mut tx, &did, &format!("{}", err))?;
                tx.commit()?;
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

    let mut env_options = heed::EnvOpenOptions::new();
    env_options
        .max_dbs(10)
        .map_size(1 * 1024 * 1024 * 1024 * 1024);
    let env = env_options.open(args.db_path)?;
    let mut tx = env.write_txn()?;
    let schema = skylight_followsdb::Schema::create(&env, &mut tx)?;
    let meta_db = env.create_database::<heed::types::CowSlice<u8>, heed::types::CowSlice<u8>>(
        &mut tx,
        Some("crawler_meta"),
    )?;
    let queued_db = env
        .create_database::<heed::types::Str, heed::types::Unit>(&mut tx, Some("crawler_queued"))?;
    let pending_db = env
        .create_database::<heed::types::Str, heed::types::Unit>(&mut tx, Some("crawler_pending"))?;
    let errored_db = env
        .create_database::<heed::types::Str, heed::types::Str>(&mut tx, Some("crawler_errored"))?;
    tx.commit()?;

    // Before we start, we should move all the pending items back into the queue as they were incompletely processed.
    {
        let mut tx = env.write_txn()?;
        let mut keys = vec![];
        {
            let mut iter = pending_db.iter_mut(&mut tx)?;
            while let Some(k) = iter.next() {
                let (k, _) = k?;
                keys.push(k.to_string());
                unsafe {
                    iter.del_current()?;
                }
            }
        }
        for k in keys {
            queued_db.put(&mut tx, &k, &())?;
        }
        tx.commit()?;
    }

    let rl = std::sync::Arc::new(governor::RateLimiter::direct(governor::Quota::per_second(
        std::num::NonZeroU32::new(3000 / (5 * 60)).unwrap(),
    )));

    let queued_notify = std::sync::Arc::new(tokio::sync::Notify::new());
    queued_notify.notify_waiters();

    let client = reqwest::Client::new();

    let workers = (0..args.num_workers)
        .map(|i| {
            tokio::spawn({
                let pds_host = args.pds_host.clone();
                let client = client.clone();
                let rl = std::sync::Arc::clone(&rl);
                let queued_notify = std::sync::Arc::clone(&queued_notify);
                let env = env.clone();
                let schema = schema.clone();
                let queued_db = queued_db.clone();
                let pending_db = pending_db.clone();
                let errored_db = errored_db.clone();
                async move {
                    worker_main(
                        pds_host,
                        client,
                        rl,
                        queued_notify,
                        env,
                        schema,
                        queued_db,
                        pending_db,
                        errored_db,
                    )
                    .instrument(tracing::info_span!("worker", i))
                    .await
                }
            })
        })
        .collect::<Vec<_>>();

    if !args.only_crawl_queued_repos {
        let mut cursor = "".to_string();
        loop {
            let mut url = format!(
                "{}/xrpc/com.atproto.sync.listRepos?limit=1000",
                args.pds_host
            );
            if cursor != "" {
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

            let mut tx = env.write_txn()?;
            for repo in output.repos {
                queued_db.put(&mut tx, &repo.did, &())?;
                queued_notify.notify_one();
            }

            let still_going = if let Some(c) = output.cursor {
                cursor = c;
                meta_db.put(&mut tx, "cursor".as_bytes(), cursor.as_bytes())?;
                true
            } else {
                false
            };
            tx.commit()?;

            if !still_going {
                break;
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
