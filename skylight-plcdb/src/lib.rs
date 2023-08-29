mod index;
pub mod writer;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("heed: {0}")]
    Heed(#[from] heed::Error),

    #[error("malformed key: {0:?}")]
    MalformedKey(Vec<u8>),
}

pub struct Schema {
    did_aka_index: index::Index,
    aka_did_index: index::Index,
}

impl Schema {
    pub fn create(env: &heed::Env, tx: &mut heed::RwTxn) -> Result<Self, Error> {
        let did_aka_index = env.create_database(tx, Some("did:aka"))?;
        let aka_did_index = env.create_database(tx, Some("aka:did"))?;

        Ok(Self {
            did_aka_index,
            aka_did_index,
        })
    }
}
