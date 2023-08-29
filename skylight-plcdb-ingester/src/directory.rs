#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Entry {
    pub did: String,
    pub operation: Operation,
    pub cid: String,
    pub nullified: bool,
    pub created_at: String,
}

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Service {
    pub r#type: String,
    pub endpoint: String,
}

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PlcOperation {
    pub rotation_keys: Vec<String>,
    pub verification_methods: std::collections::HashMap<String, String>,
    pub also_known_as: Vec<String>,
    pub services: std::collections::HashMap<String, Service>,
    pub prev: Option<String>,
    pub sig: String,
}

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PlcTombstone {
    pub prev: String,
    pub sig: String,
}

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Create {
    pub signing_key: String,
    pub recovery_key: String,
    pub handle: String,
    pub service: String,
    pub prev: (),
    pub sig: String,
}

#[derive(serde::Deserialize, Debug)]
#[serde(tag = "type")]
pub enum Operation {
    #[serde(rename = "plc_operation")]
    PlcOperation(PlcOperation),

    #[serde(rename = "plc_tombstone")]
    PlcTombstone(PlcTombstone),

    #[serde(rename = "create")]
    Create(Create),
}
