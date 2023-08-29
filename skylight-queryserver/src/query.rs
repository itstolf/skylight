pub fn resolve_did(
    schema: &skylight_plcdb::Schema,
    tx: &heed::RoTxn,
    actor: &str,
) -> Result<Option<String>, skylight_plcdb::Error> {
    if !skylight_plcdb::reader::get_akas(schema, tx, actor)?.is_empty() {
        return Ok(Some(actor.to_string()));
    }

    if let Some(did) = skylight_plcdb::reader::get_dids(schema, tx, actor)?.first() {
        return Ok(Some(did.to_string()));
    }

    if let Some(did) =
        skylight_plcdb::reader::get_dids(schema, tx, &format!("at://{}", actor))?.first()
    {
        return Ok(Some(did.to_string()));
    }

    Ok(None)
}

pub fn whois(
    schema: &skylight_plcdb::Schema,
    tx: &heed::RoTxn,
    actor: &str,
) -> Result<Option<(String, Vec<String>)>, skylight_plcdb::Error> {
    let did = if let Some(did) = resolve_did(schema, tx, actor)? {
        did
    } else {
        return Ok(None);
    };
    let akas = skylight_plcdb::reader::get_akas(schema, tx, &did)?;
    Ok(Some((did, akas)))
}

pub fn akas(
    schema: &skylight_plcdb::Schema,
    tx: &heed::RoTxn,
    dids: &[&str],
) -> Result<std::collections::HashMap<String, Vec<String>>, skylight_plcdb::Error> {
    let mut r = std::collections::HashMap::new();
    for did in dids {
        r.insert(
            did.to_string(),
            skylight_plcdb::reader::get_akas(schema, tx, *did)?,
        );
    }
    Ok(r)
}

pub fn mutuals(
    schema: &skylight_followsdb::Schema,
    tx: &heed::RoTxn,
    actor: &str,
) -> Result<Vec<String>, skylight_followsdb::Error> {
    Ok(
        skylight_followsdb::reader::get_followees(schema, tx, actor)?
            .into_iter()
            .filter_map(|subject| {
                match skylight_followsdb::reader::is_following(schema, tx, &subject, actor) {
                    Ok(true) => Some(Ok(subject)),
                    Ok(false) => None,
                    Err(e) => Some(Err(e)),
                }
            })
            .collect::<Result<Vec<String>, _>>()?,
    )
}

pub fn neighborhood(
    schema: &skylight_followsdb::Schema,
    tx: &heed::RoTxn,
    actor: &str,
) -> Result<Vec<(String, Vec<String>)>, skylight_followsdb::Error> {
    let ms = mutuals(schema, tx, actor)?;
    let ms_set = ms
        .iter()
        .cloned()
        .collect::<std::collections::HashSet<String>>();
    ms.into_iter()
        .map(|subject| {
            mutuals(schema, tx, &subject).map(|ms2| {
                (subject, {
                    ms2.into_iter()
                        .filter(|d| ms_set.contains(d.as_str()))
                        .collect::<Vec<_>>()
                })
            })
        })
        .collect::<Result<Vec<(String, Vec<String>)>, _>>()
}
