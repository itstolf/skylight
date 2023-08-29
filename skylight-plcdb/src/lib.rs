mod index;
pub mod reader;
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_delete_did() {
        let dir = tempfile::TempDir::new().unwrap();
        let env = heed::EnvOpenOptions::new()
            .max_dbs(10)
            .map_size(1 * 1024 * 1024 * 1024 * 1024)
            .open(dir.path())
            .unwrap();

        let mut tx = env.write_txn().unwrap();
        let schema = Schema::create(&env, &mut tx).unwrap();
        writer::add_did(&schema, &mut tx, "did:test", &["at://1", "at://2"][..]).unwrap();
        writer::add_did(&schema, &mut tx, "did:test2", &["at://2"][..]).unwrap();
        assert_eq!(
            reader::get_akas(&schema, &mut tx, "did:test").unwrap(),
            vec!["at://1", "at://2"]
        );
        assert_eq!(
            reader::get_dids(&schema, &mut tx, "at://1").unwrap(),
            vec!["did:test"]
        );
        assert_eq!(
            reader::get_dids(&schema, &mut tx, "at://2").unwrap(),
            vec!["did:test", "did:test2"]
        );
        writer::delete_did(&schema, &mut tx, "did:test").unwrap();
        assert_eq!(
            reader::get_akas(&schema, &mut tx, "did:test").unwrap(),
            vec![] as Vec<&str>
        );
        assert_eq!(
            reader::get_dids(&schema, &mut tx, "at://1").unwrap(),
            vec![] as Vec<&str>
        );
        assert_eq!(
            reader::get_dids(&schema, &mut tx, "at://2").unwrap(),
            vec!["did:test2"] as Vec<&str>
        );
    }
}
