pub fn add_did(
    schema: &crate::Schema,
    tx: &mut heed::RwTxn,
    did: &str,
    akas: &[&str],
) -> Result<(), crate::Error> {
    for aka in akas {
        schema
            .did_aka_index
            .put(tx, &crate::index::make_key(&[did, aka]), &())?;
        schema
            .aka_did_index
            .put(tx, &crate::index::make_key(&[aka, did]), &())?;
    }
    Ok(())
}

pub fn delete_did(
    schema: &crate::Schema,
    tx: &mut heed::RwTxn,
    did: &str,
) -> Result<(), crate::Error> {
    let mut akas = vec![];
    {
        let mut iter = schema
            .did_aka_index
            .prefix_iter_mut(tx, &crate::index::make_key_prefix(&[did]))?;

        while let Some(r) = iter.next() {
            let (key, ()) = r?;
            akas.push(
                crate::index::split_key(&key[..])
                    .ok_or_else(|| crate::Error::MalformedKey(key.to_vec()))?
                    .get(1)
                    .ok_or_else(|| crate::Error::MalformedKey(key.to_vec()))?
                    .to_string(),
            );

            unsafe {
                iter.del_current()?;
            };
        }
    }

    for aka in akas {
        schema
            .aka_did_index
            .delete(tx, &crate::index::make_key(&[&aka, did]))?;
    }

    Ok(())
}
