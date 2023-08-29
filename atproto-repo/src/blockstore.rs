use futures_util::StreamExt;

#[derive(Debug, serde::Deserialize, PartialEq, Eq)]
struct SignedCommit {
    did: String,
    version: u8,
    prev: Option<crate::dagcbor::DagCborCid>,
    data: crate::dagcbor::DagCborCid,
    sig: Vec<u8>,
}

pub struct Blockstore {
    mst: std::collections::HashMap<Vec<u8>, cid::Cid>,
    blocks: std::collections::HashMap<cid::Cid, Vec<u8>>,
}

impl Blockstore {
    pub fn get_by_cid(&self, cid: &cid::Cid) -> Option<&[u8]> {
        Some(self.blocks.get(cid)?)
    }

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        Some(self.get_by_cid(self.mst.get(key)?)?)
    }

    pub fn keys(&self) -> Keys {
        Keys(self.mst.keys())
    }

    pub fn key_and_cids(&self) -> KeyAndCids {
        KeyAndCids(self.mst.iter())
    }

    pub fn cids(&self) -> Cids {
        Cids(self.blocks.keys())
    }
}

pub struct KeyAndCids<'a>(std::collections::hash_map::Iter<'a, Vec<u8>, cid::Cid>);

impl<'a> Iterator for KeyAndCids<'a> {
    type Item = (&'a [u8], &'a cid::Cid);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(k, c)| (k.as_slice(), c))
    }
}

pub struct Keys<'a>(std::collections::hash_map::Keys<'a, Vec<u8>, cid::Cid>);

impl<'a> Iterator for Keys<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|v| v.as_slice())
    }
}

pub struct Cids<'a>(std::collections::hash_map::Keys<'a, cid::Cid, Vec<u8>>);

impl<'a> Iterator for Cids<'a> {
    type Item = &'a cid::Cid;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("ciborium: {0}")]
    Ciborium(#[from] ciborium::de::Error<std::io::Error>),

    #[error("rs-car: {0}")]
    RsCar(#[from] rs_car::CarDecodeError),

    #[error("mst: {0}")]
    Mst(#[from] crate::mst::Error),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("no roots")]
    NoRoots,

    #[error("missing root cid: {0}")]
    MissingRootCid(cid::Cid),
}

pub async fn load(
    r: &mut (impl futures_util::AsyncRead + Send + std::marker::Unpin),
    ignore_missing: bool,
) -> Result<Blockstore, Error> {
    let mut cr = rs_car::CarReader::new(r, false).await?;

    let roots = cr.header.roots.clone();
    let mut blocks = std::collections::HashMap::new();
    while let Some(item) = cr.next().await {
        let (cid, block) = item?;
        blocks.insert(cid.clone(), block);
    }

    let root_commit = roots.first().ok_or_else(|| Error::NoRoots)?;
    let commit: SignedCommit = ciborium::from_reader(std::io::Cursor::new(
        blocks
            .get(root_commit)
            .ok_or_else(|| Error::MissingRootCid(*root_commit))?,
    ))?;
    let mst = crate::mst::decode(&blocks, &commit.data.into(), ignore_missing)?;
    Ok(Blockstore { mst, blocks })
}
