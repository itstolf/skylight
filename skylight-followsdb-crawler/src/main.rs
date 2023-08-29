use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    db_path: std::path::PathBuf,

    #[arg(long, default_value = "https://bsky.social")]
    pds_host: String,
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
    unsafe {
        env_options.flags(
            heed::EnvFlags::NO_LOCK | heed::EnvFlags::NO_SYNC | heed::EnvFlags::NO_META_SYNC,
        );
    }
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
    }

    let rl = governor::RateLimiter::direct(governor::Quota::per_second(
        std::num::NonZeroU32::new(3000 / (5 * 60)).unwrap(),
    ));

    let queued_notify = tokio::sync::Notify::new();
    queued_notify.notify_waiters();

    // TODO: Spawn workers.

    let mut cursor = "".to_string();
    let client = reqwest::Client::new();
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
        pub struct Output {
            pub cursor: Option<String>,
            pub repos: Vec<Repo>,
        }

        #[derive(serde::Deserialize, Debug, Clone, PartialEq, Eq)]
        #[serde(rename_all = "camelCase")]
        pub struct Repo {
            pub did: String,
            pub head: String,
        }

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

        let done = if let Some(c) = output.cursor {
            cursor = c;
            meta_db.put(&mut tx, "cursor".as_bytes(), cursor.as_bytes())?;
            true
        } else {
            meta_db.delete(&mut tx, "cursor".as_bytes())?;
            false
        };
        tx.commit()?;

        if !done {
            break;
        }

        rl.until_ready().await;
    }

    Ok(())
}
