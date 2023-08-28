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
    Ok(env.create_database(&mut env.write_txn()?, Some(name))?)
}

pub struct Schema {
    follows_records: records::Records,
    follows_actor_subject_rkey_index: index::Index,
    follows_subject_actor_rkey_index: index::Index,
}

impl Schema {
    pub fn open_or_create(env: &heed::Env) -> Result<Self, Error> {
        let follows_records = open_or_create_database(&env, "follows")?;
        let follows_actor_subject_rkey_index =
            open_or_create_database(&env, "follows:actor:subject:rkey")?;
        let follows_subject_actor_rkey_index =
            open_or_create_database(&env, "follows:subject:actor:rkey")?;
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

        let schema = Schema::open_or_create(&env).unwrap();
        let mut tx = env.write_txn().unwrap();
        writer::add_follow(&schema, &mut tx, "test", "user1", "user2").unwrap();
        assert_eq!(
            reader::get_followers(&schema, &mut tx, "user1").unwrap(),
            vec!["user2"]
        );
        assert_eq!(
            reader::get_followers(&schema, &mut tx, "user2").unwrap(),
            vec![] as Vec<&str>
        );
        assert_eq!(
            reader::get_followees(&schema, &mut tx, "user1").unwrap(),
            vec![] as Vec<&str>
        );
        assert_eq!(
            reader::get_followees(&schema, &mut tx, "user2").unwrap(),
            vec!["user1"]
        );
        writer::delete_actor(&schema, &mut tx, "user1").unwrap();
        assert_eq!(
            reader::get_followees(&schema, &mut tx, "user2").unwrap(),
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

        let schema = Schema::open_or_create(&env).unwrap();
        let mut tx = env.write_txn().unwrap();
        writer::add_follow(&schema, &mut tx, "test", "user1", "user2").unwrap();
        writer::add_follow(&schema, &mut tx, "test2", "user2", "user1").unwrap();
        assert_eq!(
            reader::get_followers(&schema, &mut tx, "user1").unwrap(),
            vec!["user2"]
        );
        assert_eq!(
            reader::get_followers(&schema, &mut tx, "user2").unwrap(),
            vec!["user1"] as Vec<&str>
        );
        assert_eq!(
            reader::get_followees(&schema, &mut tx, "user1").unwrap(),
            vec!["user2"] as Vec<&str>
        );
        assert_eq!(
            reader::get_followees(&schema, &mut tx, "user2").unwrap(),
            vec!["user1"]
        );
        writer::delete_follow(&schema, &mut tx, "test").unwrap();
        assert_eq!(
            reader::get_followers(&schema, &mut tx, "user1").unwrap(),
            vec![] as Vec<&str>
        );
        assert_eq!(
            reader::get_followers(&schema, &mut tx, "user2").unwrap(),
            vec!["user1"] as Vec<&str>
        );
        assert_eq!(
            reader::get_followees(&schema, &mut tx, "user1").unwrap(),
            vec!["user2"] as Vec<&str>
        );
        assert_eq!(
            reader::get_followees(&schema, &mut tx, "user2").unwrap(),
            vec![] as Vec<&str>
        );
    }
}
