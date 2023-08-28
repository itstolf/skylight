pub fn get_followers(
    db: &crate::Db,
    tx: &heed::RoTxn,
    actor: &str,
) -> Result<Vec<String>, crate::Error> {
    db.follows_actor_subject_rkey_index
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

pub fn get_followees(
    db: &crate::Db,
    tx: &heed::RoTxn,
    actor: &str,
) -> Result<Vec<String>, crate::Error> {
    db.follows_subject_actor_rkey_index
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
