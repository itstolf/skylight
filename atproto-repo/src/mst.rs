#[derive(Debug, serde::Deserialize, PartialEq, Eq)]
struct Entry {
    #[serde(rename = "p")]
    prefix_len: u32,

    #[serde(rename = "k")]
    key_suffix: Vec<u8>,

    #[serde(rename = "v")]
    value: crate::dagcbor::DagCborCid,

    #[serde(rename = "t")]
    right: Option<crate::dagcbor::DagCborCid>,
}

#[derive(Debug, serde::Deserialize, PartialEq)]
struct Node {
    #[serde(rename = "l")]
    left: Option<crate::dagcbor::DagCborCid>,

    #[serde(rename = "e")]
    entries: Vec<Entry>,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("ciborium: {0}")]
    Ciborium(#[from] ciborium::de::Error<std::io::Error>),

    #[error("missing cid: {0}")]
    MissingCid(cid::Cid),
}

pub fn decode(
    blocks: &std::collections::HashMap<cid::Cid, Vec<u8>>,
    cid: &cid::Cid,
    ignore_missing: bool,
) -> Result<std::collections::HashMap<Vec<u8>, cid::Cid>, Error> {
    let mut mst = std::collections::HashMap::new();

    let block = if let Some(block) = blocks.get(cid) {
        block
    } else {
        if ignore_missing {
            return Ok(mst);
        }
        return Err(Error::MissingCid(cid.clone()));
    };

    let node: Node = ciborium::from_reader(std::io::Cursor::new(block))?;
    if let Some(left) = &node.left {
        mst.extend(decode(blocks, left.into(), ignore_missing)?);
    }

    let mut key = vec![];
    for entry in node.entries.iter() {
        key = key[..entry.prefix_len as usize]
            .iter()
            .cloned()
            .chain(entry.key_suffix.iter().cloned())
            .collect::<Vec<u8>>();
        mst.insert(key.clone(), entry.value.clone().into());
        if let Some(right) = &entry.right {
            mst.extend(decode(blocks, right.into(), ignore_missing)?);
        }
    }

    Ok(mst)
}
