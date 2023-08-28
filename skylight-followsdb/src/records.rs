pub type Records = heed::Database<heed::types::CowSlice<u8>, heed::types::CowSlice<u8>>;

pub fn make_record(actor: &str, subject: &str) -> Vec<u8> {
    [actor.as_bytes(), subject.as_bytes()].join(&b"\0"[..])
}

pub fn parse_record(k: &[u8]) -> Option<(String, String)> {
    Some(
        match &k
            .splitn(2, |c| *c == '\0' as u8)
            .map(|v| String::from_utf8(v.to_vec()).ok())
            .collect::<Option<Vec<_>>>()?[..]
        {
            [actor, subject] => (actor.clone(), subject.clone()),
            _ => {
                return None;
            }
        },
    )
}
