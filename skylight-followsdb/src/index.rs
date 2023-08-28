pub fn split_key(k: &[u8]) -> Option<Vec<String>> {
    k.split(|c| *c == '\0' as u8)
        .map(|v| String::from_utf8(v.to_vec()).ok())
        .collect()
}

pub fn make_key(parts: &[&str]) -> Vec<u8> {
    parts
        .iter()
        .map(|p| p.as_bytes())
        .collect::<Vec<_>>()
        .join(&b"\0"[..])
}

pub fn make_key_prefix(parts: &[&str]) -> Vec<u8> {
    let mut prefix = make_key(parts);
    prefix.push('\0' as u8);
    prefix
}

pub type Index = heed::Database<heed::types::CowSlice<u8>, heed::types::Unit>;

fn open_or_create_index(env: &heed::Env, name: &str) -> Result<Index, crate::Error> {
    Ok(crate::open_or_create_database(env, name)?)
}

const FOLLOWS_ACTOR_SUBJECT_RKEY_INDEX: &str = "follows:actor:subject:rkey";
const FOLLOWS_SUBJECT_ACTOR_RKEY_INDEX: &str = "follows:subject:actor:rkey";

pub fn open_or_create_follows_actor_subject_rkey_index(
    env: &heed::Env,
) -> Result<Index, crate::Error> {
    open_or_create_index(env, FOLLOWS_ACTOR_SUBJECT_RKEY_INDEX)
}

pub fn open_or_create_follows_subject_actor_rkey_index(
    env: &heed::Env,
) -> Result<Index, crate::Error> {
    open_or_create_index(env, FOLLOWS_SUBJECT_ACTOR_RKEY_INDEX)
}

pub fn initialize(env: &heed::Env) -> Result<(), crate::Error> {
    let _: Index = env.create_database(Some(FOLLOWS_ACTOR_SUBJECT_RKEY_INDEX))?;
    let _: Index = env.create_database(Some(FOLLOWS_SUBJECT_ACTOR_RKEY_INDEX))?;
    Ok(())
}
