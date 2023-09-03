#[derive(serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Commit {
    pub blobs: Vec<atproto_repo::dagcbor::DagCborCid>,
    #[serde(with = "serde_bytes")]
    pub blocks: Vec<u8>,
    pub commit: Option<atproto_repo::dagcbor::DagCborCid>,
    pub ops: Vec<RepoOp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev: Option<atproto_repo::dagcbor::DagCborCid>,
    pub rebase: bool,
    pub repo: String,
    pub seq: i64,
    pub time: String,
    pub too_big: bool,
}

#[derive(serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Handle {
    pub did: String,
    pub handle: String,
    pub seq: i64,
    pub time: String,
}

#[derive(serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Info {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub name: String,
}

#[derive(serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Migrate {
    pub did: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub migrate_to: Option<String>,
    pub seq: i64,
    pub time: String,
}

#[derive(serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RepoOp {
    pub action: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cid: Option<atproto_repo::dagcbor::DagCborCid>,
    pub path: String,
}

#[derive(serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Tombstone {
    pub did: String,
    pub seq: i64,
    pub time: String,
}

pub enum Message {
    Commit(Commit),
    Handle(Handle),
    Info(Info),
    Migrate(Migrate),
    Tombstone(Tombstone),
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("firehose: {error}: {message:?}")]
    Firehose {
        error: String,
        message: Option<String>,
    },

    #[error("ciborium: {0}")]
    Ciborium(#[from] ciborium::de::Error<std::io::Error>),

    #[error("unknown operation: {0}")]
    UnknownOperation(i8),

    #[error("unknown type: {0}")]
    UnknownType(String),
}

impl Message {
    pub fn parse(buf: &[u8]) -> Result<Self, Error> {
        let mut cursor = std::io::Cursor::new(buf);

        #[derive(serde::Deserialize)]
        struct Header {
            #[serde(rename = "op")]
            operation: i8,

            #[serde(rename = "t")]
            r#type: Option<String>,
        }
        let Header { operation, r#type } = ciborium::from_reader(&mut cursor)?;

        if operation == -1 {
            #[derive(serde::Deserialize)]
            struct ErrorBody {
                error: String,
                message: Option<String>,
            }
            let ErrorBody { error, message } = ciborium::from_reader(&mut cursor)?;
            return Err(Error::Firehose { error, message });
        }

        if operation != 1 {
            return Err(Error::UnknownOperation(operation));
        }

        Ok(match r#type.unwrap_or_else(|| "".to_string()).as_str() {
            "#commit" => Self::Commit(ciborium::from_reader(&mut cursor)?),
            "#handle" => Self::Handle(ciborium::from_reader(&mut cursor)?),
            "#info" => Self::Info(ciborium::from_reader(&mut cursor)?),
            "#migrate" => Self::Migrate(ciborium::from_reader(&mut cursor)?),
            "#tombstone" => Self::Tombstone(ciborium::from_reader(&mut cursor)?),
            t => return Err(Error::UnknownType(t.to_string())),
        })
    }
}
