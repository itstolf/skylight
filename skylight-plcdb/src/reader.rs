pub fn get_dids(
    schema: &crate::Schema,
    tx: &heed::RoTxn,
    aka: &str,
) -> Result<Vec<String>, crate::Error> {
    schema
        .aka_did_index
        .prefix_iter(tx, &crate::index::make_key_prefix(&[aka]))?
        .map(|r| {
            r.map_err(|e| e.into()).and_then(|(k, ())| {
                crate::index::split_key(&k)
                    .and_then(|k| k.get(1).cloned())
                    .ok_or_else(|| crate::Error::MalformedKey(k.to_vec()))
            })
        })
        .collect()
}

pub fn get_akas(
    schema: &crate::Schema,
    tx: &heed::RoTxn,
    did: &str,
) -> Result<Vec<String>, crate::Error> {
    schema
        .did_aka_index
        .prefix_iter(tx, &crate::index::make_key_prefix(&[did]))?
        .map(|r| {
            r.map_err(|e| e.into()).and_then(|(k, ())| {
                crate::index::split_key(&k)
                    .and_then(|k| k.get(1).cloned())
                    .ok_or_else(|| crate::Error::MalformedKey(k.to_vec()))
            })
        })
        .collect()
}
