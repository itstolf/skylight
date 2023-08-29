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

pub fn mutuals<'txn>(
    schema: &'txn skylight_followsdb::Schema,
    tx: &'txn heed::RoTxn,
    actor: &'txn str,
) -> Result<
    impl Iterator<Item = Result<String, skylight_followsdb::Error>> + 'txn,
    skylight_followsdb::Error,
> {
    Ok(
        skylight_followsdb::reader::get_followees(schema, tx, actor)?.filter_map(|subject| {
            let subject = match subject {
                Ok(subject) => subject,
                Err(e) => {
                    return Some(Err(e));
                }
            };
            match skylight_followsdb::reader::is_following(schema, tx, &subject, actor) {
                Ok(true) => Some(Ok(subject)),
                Ok(false) => None,
                Err(e) => Some(Err(e)),
            }
        }),
    )
}

pub fn neighborhood(
    schema: &skylight_followsdb::Schema,
    tx: &heed::RoTxn,
    did: &str,
) -> Result<Vec<(String, Vec<String>)>, skylight_followsdb::Error> {
    let ms = mutuals(schema, tx, did)?.collect::<Result<Vec<_>, _>>()?;
    let ms_set = ms
        .iter()
        .cloned()
        .collect::<std::collections::HashSet<String>>();
    ms.into_iter()
        .map(|subject| {
            mutuals(schema, tx, &subject).and_then(|ms2| {
                ms2.filter_map(|d| match d {
                    Ok(d) => {
                        if ms_set.contains(d.as_str()) {
                            Some(Ok(d))
                        } else {
                            None
                        }
                    }
                    Err(e) => Some(Err(e)),
                })
                .collect::<Result<Vec<_>, _>>()
                .map(|ms2| (subject.clone(), ms2))
            })
        })
        .collect::<Result<Vec<(String, Vec<String>)>, _>>()
}

fn build_path(
    node: &str,
    source_parents: &std::collections::HashMap<String, Option<String>>,
    target_parents: &std::collections::HashMap<String, Option<String>>,
) -> Vec<String> {
    let mut path = vec![];
    let mut node = Some(node.to_string());

    while let Some(n) = node.as_ref() {
        path.push(n.clone());
        node = source_parents.get(n).cloned().flatten();
    }
    path.reverse();

    node = path
        .last()
        .and_then(|v| target_parents.get(v))
        .and_then(|v| v.as_ref())
        .cloned();
    while let Some(n) = node.as_ref() {
        path.push(n.clone());
        node = target_parents.get(n).cloned().flatten();
    }

    path
}

pub fn find_mutuals_path(
    schema: &skylight_followsdb::Schema,
    tx: &heed::RoTxn,
    source: &str,
    target: &str,
    ignore: std::collections::HashSet<&str>,
    max_depth: usize,
    max_mutuals: usize,
) -> Result<Option<Vec<String>>, skylight_followsdb::Error> {
    if source == target {
        return Ok(Some(vec![source.to_string()]));
    }

    let mut source_q = std::collections::VecDeque::from([(source.to_string(), 0usize)]);
    let mut source_visited = std::collections::HashMap::from([(source.to_string(), None)]);

    let mut target_q = std::collections::VecDeque::from([(target.to_string(), 0usize)]);
    let mut target_visited = std::collections::HashMap::from([(target.to_string(), None)]);

    while !source_q.is_empty() && !target_q.is_empty() {
        let (q, other_q, visited, other_visited) = if source_q.len() <= target_q.len() {
            (
                &mut source_q,
                &mut target_q,
                &mut source_visited,
                &mut target_visited,
            )
        } else {
            (
                &mut target_q,
                &mut source_q,
                &mut target_visited,
                &mut source_visited,
            )
        };

        let (did, depth) = q.pop_front().unwrap();
        let (_, other_depth) = other_q.front().unwrap();

        if depth + 1 + *other_depth >= max_depth {
            return Ok(None);
        }

        let muts = mutuals(schema, tx, &did)?.collect::<Result<Vec<String>, _>>()?;
        if max_mutuals > 0 && muts.len() > max_mutuals {
            continue;
        }
        for neighbor in muts {
            if ignore.contains(neighbor.as_str()) {
                continue;
            }

            if visited.contains_key(&neighbor) {
                continue;
            }
            visited.insert(neighbor.clone(), Some(did.clone()));
            // nodes_expanded += 1;

            q.push_back((neighbor.clone(), depth + 1));

            if other_visited.contains_key(&neighbor) {
                if source_q.len() <= target_q.len() {
                    return Ok(Some(build_path(
                        &neighbor,
                        &source_visited,
                        &target_visited,
                    )));
                } else {
                    let mut path = build_path(&neighbor, &target_visited, &source_visited);
                    path.reverse();
                    return Ok(Some(path));
                }
            }
        }
    }

    Ok(Some(vec![]))
}
