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

    let rl = governor::RateLimiter::direct(governor::Quota::per_second(
        std::num::NonZeroU32::new(3000 / (5 * 60)).unwrap(),
    ));

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

        for repo in output.repos {
            tracing::info!(repo = repo.did);
        }

        if let Some(c) = output.cursor {
            cursor = c;
        } else {
            break;
        }
        rl.until_ready().await;
    }

    Ok(())
}
