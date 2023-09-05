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

#[derive(Clone)]
pub struct Decoder {
    ignore_missing: bool,
}

impl Decoder {
    pub fn new() -> Self {
        Self {
            ignore_missing: false,
        }
    }

    pub fn ignore_missing(&mut self, ignore_missing: bool) -> &mut Self {
        self.ignore_missing = ignore_missing;
        self
    }

    pub fn decode(
        &self,
        blocks: &std::collections::HashMap<cid::Cid, Vec<u8>>,
        cid: &cid::Cid,
    ) -> Result<std::collections::HashMap<Vec<u8>, cid::Cid>, Error> {
        let mut mst = std::collections::HashMap::new();

        let block = if let Some(block) = blocks.get(cid) {
            block
        } else {
            if self.ignore_missing {
                return Ok(mst);
            }
            return Err(Error::MissingCid(cid.clone()));
        };

        let node: Node = ciborium::from_reader(std::io::Cursor::new(block))?;
        if let Some(left) = &node.left {
            mst.extend(self.decode(blocks, left.into())?);
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
                mst.extend(self.decode(blocks, right.into())?);
            }
        }

        Ok(mst)
    }
}
