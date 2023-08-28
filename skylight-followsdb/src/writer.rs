pub fn add_follow(
    db: &crate::Db,
    tx: &mut heed::RwTxn,
    rkey: &str,
    actor: &str,
    subject: &str,
) -> Result<(), crate::Error> {
    db.follows_records.put(
        tx,
        rkey.as_bytes(),
        &crate::records::make_record(actor, subject)[..],
    )?;
    db.follows_actor_subject_rkey_index.put(
        tx,
        &crate::index::make_key(&[actor, subject, rkey])[..],
        &(),
    )?;
    db.follows_subject_actor_rkey_index.put(
        tx,
        &crate::index::make_key(&[subject, actor, rkey])[..],
        &(),
    )?;

    Ok(())
}

pub fn delete_follow(db: &crate::Db, tx: &mut heed::RwTxn, rkey: &str) -> Result<(), crate::Error> {
    let raw = if let Some(raw) = db.follows_records.get(tx, rkey.as_bytes())? {
        raw
    } else {
        return Ok(());
    };

    let (actor, subject) = crate::records::parse_record(&raw)
        .ok_or_else(|| crate::Error::MalformedRecord(rkey.to_string()))?;

    db.follows_records.delete(tx, rkey.as_bytes())?;
    db.follows_actor_subject_rkey_index
        .delete(tx, &crate::index::make_key(&[&actor, &subject, rkey]))?;
    db.follows_subject_actor_rkey_index
        .delete(tx, &crate::index::make_key(&[&subject, &actor, rkey]))?;

    Ok(())
}

fn prune_index(
    tx: &mut heed::RwTxn,
    index: &crate::index::Index,
    prefix: &[u8],
) -> Result<Vec<String>, crate::Error> {
    let mut rkeys = vec![];

    let mut iter = index.prefix_iter_mut(tx, &prefix)?;
    while let Some(r) = iter.next() {
        let (key, ()) = r?;
        rkeys.push(
            crate::index::split_key(&key[..])
                .ok_or_else(|| crate::Error::MalformedKey(key.to_vec()))?
                .get(2)
                .ok_or_else(|| crate::Error::MalformedKey(key.to_vec()))?
                .to_string(),
        );
        iter.del_current()?;
    }

    Ok(rkeys)
}

pub fn delete_actor(db: &crate::Db, tx: &mut heed::RwTxn, actor: &str) -> Result<(), crate::Error> {
    let prefix = crate::index::make_key_prefix(&[actor]);

    for rkey in std::iter::empty()
        .chain(prune_index(
            tx,
            &db.follows_actor_subject_rkey_index,
            &prefix,
        )?)
        .chain(prune_index(
            tx,
            &db.follows_subject_actor_rkey_index,
            &prefix,
        )?)
    {
        let (actor, subject) = crate::records::parse_record(
            &db.follows_records
                .get(tx, rkey.as_bytes())?
                .ok_or_else(|| crate::Error::MalformedKey(rkey.as_bytes().to_vec()))?,
        )
        .ok_or_else(|| crate::Error::MalformedRecord(rkey.to_string()))?;
        db.follows_records.delete(tx, rkey.as_bytes())?;
        db.follows_actor_subject_rkey_index
            .delete(tx, &crate::index::make_key(&[&actor, &subject, &rkey]))?;
        db.follows_subject_actor_rkey_index
            .delete(tx, &crate::index::make_key(&[&subject, &actor, &rkey]))?;
    }

    Ok(())
}
