mod firehose;
use byteorder::ByteOrder;
use clap::Parser;
use futures::{SinkExt, StreamExt};
use tracing::Instrument;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    db_path: std::path::PathBuf,

    #[arg(long, default_value = "wss://bsky.social")]
    firehose_host: String,
}

type MetaDB = heed::Database<heed::types::CowSlice<u8>, heed::types::CowSlice<u8>>;

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
    let meta_db: MetaDB = env.create_database(&mut tx, Some("ingester_meta"))?;
    tx.commit()?;

    let cursor = {
        let tx = env.read_txn()?;
        meta_db
            .get(&tx, "cursor".as_bytes())?
            .map(|v| byteorder::LittleEndian::read_i64(&v))
            .unwrap_or(-1)
    };
    tracing::info!(message = "cursor", cursor = cursor);

    let mut url = format!(
        "{}/xrpc/com.atproto.sync.subscribeRepos",
        args.firehose_host
    );
    if cursor >= 0 {
        url.push_str(&format!("?cursor={cursor}"));
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
                process_message(&env, &schema, &meta_db, &msg)
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

async fn process_message(
    env: &heed::Env,
    schema: &skylight_followsdb::Schema,
    meta_db: &MetaDB,
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

    let mut tx = env.write_txn()?;
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

                let items = match rs_car::car_read_all(&mut commit.blocks.as_slice(), true).await {
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

                        // Crash if we can't write to followsdb.
                        skylight_followsdb::writer::add_follow(
                            schema,
                            &mut tx,
                            rkey,
                            &commit.repo,
                            &record.subject,
                        )
                        .expect("skylight_followsdb::writer::add_follow");
                        tracing::info!(
                            action = "create follow",
                            seq = commit.seq,
                            actor_did = commit.repo,
                            subject_did = record.subject,
                            rkey = rkey
                        )
                    }
                    "delete" => {
                        // Crash if we can't write to followsdb.
                        skylight_followsdb::writer::delete_follow(schema, &mut tx, rkey)
                            .expect("skylight_followsdb::writer::delete_follow");
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
            // Crash if we can't write to followsdb.
            skylight_followsdb::writer::delete_actor(schema, &mut tx, &tombstone.did)
                .expect("skylight_followsdb::writer::delete_actor");
            tracing::info!(
                action = "delete actor",
                seq = tombstone.seq,
                actor_did = tombstone.did
            );
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
    let mut buf = [0u8; 8];
    byteorder::LittleEndian::write_i64(&mut buf, seq);
    // Crash if we can't write the cursor.
    meta_db
        .put(&mut tx, "cursor".as_bytes(), &buf)
        .expect("write cursor");
    tx.commit()?;
    Ok(())
}