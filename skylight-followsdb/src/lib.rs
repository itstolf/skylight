mod index;
pub mod reader;
mod records;
pub mod writer;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("heed: {0}")]
    Heed(#[from] heed::Error),

    #[error("malformed key: {0:?}")]
    MalformedKey(Vec<u8>),

    #[error("malformed record: {0:?}")]
    MalformedRecord(String),

    #[error("missing database: {0}")]
    MissingDatabase(String),
}

#[derive(Clone)]
pub struct Schema {
    pub follows_records: records::Records,
    pub follows_actor_subject_rkey_index: index::Index,
    pub follows_subject_actor_rkey_index: index::Index,
}

impl Schema {
    pub fn create(env: &heed::Env, tx: &mut heed::RwTxn) -> Result<Self, Error> {
        let follows_records = env.create_database(tx, Some("follows"))?;
        let follows_actor_subject_rkey_index =
            env.create_database(tx, Some("follows:actor:subject:rkey"))?;
        let follows_subject_actor_rkey_index =
            env.create_database(tx, Some("follows:subject:actor:rkey"))?;

        Ok(Self {
            follows_records,
            follows_actor_subject_rkey_index,
            follows_subject_actor_rkey_index,
        })
    }

    pub fn open(env: &heed::Env, tx: &heed::RoTxn) -> Result<Self, Error> {
        let follows_records = env
            .open_database(tx, Some("follows"))?
            .ok_or_else(|| Error::MissingDatabase("follows".to_string()))?;
        let follows_actor_subject_rkey_index = env
            .open_database(tx, Some("follows:actor:subject:rkey"))?
            .ok_or_else(|| Error::MissingDatabase("follows:actor:subject:rkey".to_string()))?;
        let follows_subject_actor_rkey_index = env
            .open_database(tx, Some("follows:subject:actor:rkey"))?
            .ok_or_else(|| Error::MissingDatabase("follows:subject:actor:rkey".to_string()))?;

        Ok(Self {
            follows_records,
            follows_actor_subject_rkey_index,
            follows_subject_actor_rkey_index,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_delete_actor() {
        let dir = tempfile::TempDir::new().unwrap();
        let env = heed::EnvOpenOptions::new()
            .max_dbs(10)
            .map_size(1 * 1024 * 1024 * 1024 * 1024)
            .open(dir.path())
            .unwrap();

        let mut tx = env.write_txn().unwrap();
        let schema = Schema::create(&env, &mut tx).unwrap();
        writer::add_follow(&schema, &mut tx, "test", "user1", "user2").unwrap();
        assert_eq!(
            reader::get_followees(&schema, &mut tx, "user1")
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            vec!["user2"]
        );
        assert_eq!(
            reader::get_followees(&schema, &mut tx, "user2")
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            vec![] as Vec<&str>
        );
        assert_eq!(
            reader::get_followers(&schema, &mut tx, "user1")
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            vec![] as Vec<&str>
        );
        assert_eq!(
            reader::get_followers(&schema, &mut tx, "user2")
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            vec!["user1"]
        );
        writer::delete_actor(&schema, &mut tx, "user1").unwrap();
        assert_eq!(
            reader::get_followers(&schema, &mut tx, "user2")
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            vec![] as Vec<&str>
        );
    }

    #[test]
    fn test_delete_follow() {
        let dir = tempfile::TempDir::new().unwrap();
        let env = heed::EnvOpenOptions::new()
            .max_dbs(10)
            .map_size(1 * 1024 * 1024 * 1024 * 1024)
            .open(dir.path())
            .unwrap();

        let mut tx = env.write_txn().unwrap();
        let schema = Schema::create(&env, &mut tx).unwrap();
        writer::add_follow(&schema, &mut tx, "test", "user1", "user2").unwrap();
        writer::add_follow(&schema, &mut tx, "test2", "user2", "user1").unwrap();
        assert_eq!(
            reader::get_followees(&schema, &mut tx, "user1")
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            vec!["user2"]
        );
        assert_eq!(
            reader::get_followees(&schema, &mut tx, "user2")
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            vec!["user1"] as Vec<&str>
        );
        assert_eq!(
            reader::get_followers(&schema, &mut tx, "user1")
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            vec!["user2"] as Vec<&str>
        );
        assert_eq!(
            reader::get_followers(&schema, &mut tx, "user2")
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            vec!["user1"]
        );
        writer::delete_follow(&schema, &mut tx, "test").unwrap();
        assert_eq!(
            reader::get_followees(&schema, &mut tx, "user1")
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            vec![] as Vec<&str>
        );
        assert_eq!(
            reader::get_followees(&schema, &mut tx, "user2")
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            vec!["user1"] as Vec<&str>
        );
        assert_eq!(
            reader::get_followers(&schema, &mut tx, "user1")
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            vec!["user2"] as Vec<&str>
        );
        assert_eq!(
            reader::get_followers(&schema, &mut tx, "user2")
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            vec![] as Vec<&str>
        );
    }
}
