#[derive(serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Commit {
    pub blobs: Vec<atproto_repo::dagcbor::DagCborCid>,
    #[doc = "CAR file containing relevant blocks"]
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
