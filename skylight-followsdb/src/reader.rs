pub fn get_followees(
    schema: &crate::Schema,
    tx: &heed::RoTxn,
    actor: &str,
) -> Result<Vec<String>, crate::Error> {
    schema
        .follows_actor_subject_rkey_index
        .prefix_iter(tx, &crate::index::make_key_prefix(&[actor]))?
        .map(|r| {
            r.map_err(|e| e.into()).and_then(|(k, ())| {
                crate::index::split_key(&k)
                    .and_then(|k| k.get(1).cloned())
                    .ok_or_else(|| crate::Error::MalformedKey(k.to_vec()))
            })
        })
        .collect()
}

pub fn get_followers(
    schema: &crate::Schema,
    tx: &heed::RoTxn,
    actor: &str,
) -> Result<Vec<String>, crate::Error> {
    schema
        .follows_subject_actor_rkey_index
        .prefix_iter(tx, &crate::index::make_key_prefix(&[actor]))?
        .map(|r| {
            r.map_err(|e| e.into()).and_then(|(k, ())| {
                crate::index::split_key(&k)
                    .and_then(|k| k.get(1).cloned())
                    .ok_or_else(|| crate::Error::MalformedKey(k.to_vec()))
            })
        })
        .collect()
}

pub fn is_following(
    schema: &crate::Schema,
    tx: &heed::RoTxn,
    actor: &str,
    subject: &str,
) -> Result<bool, crate::Error> {
    Ok(schema
        .follows_actor_subject_rkey_index
        .prefix_iter(tx, &crate::index::make_key_prefix(&[actor, subject]))?
        .next()
        .is_some())
}
