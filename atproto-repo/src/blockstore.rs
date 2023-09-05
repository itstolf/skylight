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

    pub fn keys(&self) -> impl Iterator<Item = &[u8]> {
        self.mst.keys().map(|v| &v[..])
    }

    pub fn key_and_cids(&self) -> impl Iterator<Item = (&[u8], &cid::Cid)> {
        self.mst.iter().map(|(k, c)| (&k[..], c))
    }

    pub fn cids(&self) -> impl Iterator<Item = &cid::Cid> {
        self.blocks.keys()
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

#[derive(Clone)]
pub struct Loader {
    validate_block_hash: bool,
    mst_ignore_missing: bool,
}

impl Loader {
    pub fn new() -> Self {
        Self {
            validate_block_hash: false,
            mst_ignore_missing: false,
        }
    }

    pub fn mst_ignore_missing(&mut self, mst_ignore_missing: bool) -> &mut Self {
        self.mst_ignore_missing = mst_ignore_missing;
        self
    }

    pub fn validate_block_hash(&mut self, validate_block_hash: bool) -> &mut Self {
        self.validate_block_hash = validate_block_hash;
        self
    }

    pub async fn load(
        &self,
        r: &mut (impl futures_util::AsyncRead + Send + std::marker::Unpin),
    ) -> Result<Blockstore, Error> {
        let mut cr = rs_car::CarReader::new(r, self.validate_block_hash).await?;

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
        let mst = crate::mst::Decoder::new()
            .ignore_missing(self.mst_ignore_missing)
            .decode(&blocks, &commit.data.into())?;
        Ok(Blockstore { mst, blocks })
    }
}
