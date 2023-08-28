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
}

fn open_or_create_database<K, V>(
    env: &heed::Env,
    name: &str,
) -> Result<heed::Database<K, V>, crate::Error>
where
    K: 'static,
    V: 'static,
{
    if let Some(db) = env.open_database(Some(name))? {
        return Ok(db);
    }
    Ok(env.create_database(Some(name))?)
}

pub struct Db {
    env: heed::Env,
    follows_records: records::Records,
    follows_actor_subject_rkey_index: index::Index,
    follows_subject_actor_rkey_index: index::Index,
}

impl Db {
    pub fn open_or_create(path: &std::path::Path) -> Result<Self, Error> {
        let env = heed::EnvOpenOptions::new().max_dbs(10).open(path)?;
        let follows_records = open_or_create_database(&env, "follows")?;
        let follows_actor_subject_rkey_index =
            open_or_create_database(&env, "follows:actor:subject:rkey")?;
        let follows_subject_actor_rkey_index =
            open_or_create_database(&env, "follows:subject:actor:rkey")?;
        Ok(Db {
            env,
            follows_records,
            follows_actor_subject_rkey_index,
            follows_subject_actor_rkey_index,
        })
    }

    pub fn read_txn(&self) -> Result<heed::RoTxn, Error> {
        Ok(self.env.read_txn()?)
    }

    pub fn write_txn(&self) -> Result<heed::RwTxn, Error> {
        Ok(self.env.write_txn()?)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_delete_actor() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = Db::open_or_create(dir.path()).unwrap();
        let mut tx = db.write_txn().unwrap();
        writer::add_follow(&db, &mut tx, "test", "user1", "user2").unwrap();
        assert_eq!(
            reader::get_followers(&db, &mut tx, "user1").unwrap(),
            vec!["user2"]
        );
        assert_eq!(
            reader::get_followers(&db, &mut tx, "user2").unwrap(),
            vec![] as Vec<&str>
        );
        assert_eq!(
            reader::get_followees(&db, &mut tx, "user1").unwrap(),
            vec![] as Vec<&str>
        );
        assert_eq!(
            reader::get_followees(&db, &mut tx, "user2").unwrap(),
            vec!["user1"]
        );
        writer::delete_actor(&db, &mut tx, "user1").unwrap();
        assert_eq!(
            reader::get_followees(&db, &mut tx, "user2").unwrap(),
            vec![] as Vec<&str>
        );
    }

    #[test]
    fn test_delete_follow() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = Db::open_or_create(dir.path()).unwrap();
        let mut tx = db.write_txn().unwrap();
        writer::add_follow(&db, &mut tx, "test", "user1", "user2").unwrap();
        writer::add_follow(&db, &mut tx, "test2", "user2", "user1").unwrap();
        assert_eq!(
            reader::get_followers(&db, &mut tx, "user1").unwrap(),
            vec!["user2"]
        );
        assert_eq!(
            reader::get_followers(&db, &mut tx, "user2").unwrap(),
            vec!["user1"] as Vec<&str>
        );
        assert_eq!(
            reader::get_followees(&db, &mut tx, "user1").unwrap(),
            vec!["user2"] as Vec<&str>
        );
        assert_eq!(
            reader::get_followees(&db, &mut tx, "user2").unwrap(),
            vec!["user1"]
        );
        writer::delete_follow(&db, &mut tx, "test").unwrap();
        assert_eq!(
            reader::get_followers(&db, &mut tx, "user1").unwrap(),
            vec![] as Vec<&str>
        );
        assert_eq!(
            reader::get_followers(&db, &mut tx, "user2").unwrap(),
            vec!["user1"] as Vec<&str>
        );
        assert_eq!(
            reader::get_followees(&db, &mut tx, "user1").unwrap(),
            vec!["user2"] as Vec<&str>
        );
        assert_eq!(
            reader::get_followees(&db, &mut tx, "user2").unwrap(),
            vec![] as Vec<&str>
        );
    }
}
