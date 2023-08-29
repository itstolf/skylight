pub fn get_followees<'txn>(
    schema: &crate::Schema,
    tx: &'txn heed::RoTxn,
    actor: &str,
) -> Result<impl Iterator<Item = Result<String, crate::Error>> + 'txn, crate::Error> {
    Ok(schema
        .follows_actor_subject_rkey_index
        .prefix_iter(tx, &crate::index::make_key_prefix(&[actor]))?
        .map(|r| {
            r.map_err(|e| e.into()).and_then(|(k, ())| {
                crate::index::split_key(&k)
                    .and_then(|k: Vec<String>| k.get(1).cloned())
                    .ok_or_else(|| crate::Error::MalformedKey(k.to_vec()))
            })
        }))
}

pub fn get_followers<'txn>(
    schema: &crate::Schema,
    tx: &'txn heed::RoTxn,
    actor: &str,
) -> Result<impl Iterator<Item = Result<String, crate::Error>> + 'txn, crate::Error> {
    Ok(schema
        .follows_subject_actor_rkey_index
        .prefix_iter(tx, &crate::index::make_key_prefix(&[actor]))?
        .map(|r| {
            r.map_err(|e| e.into()).and_then(|(k, ())| {
                crate::index::split_key(&k)
                    .and_then(|k| k.get(1).cloned())
                    .ok_or_else(|| crate::Error::MalformedKey(k.to_vec()))
            })
        }))
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
