use std::io::Read;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DagCborCidGeneric<const S: usize>(cid::CidGeneric<S>);

pub type DagCborCid = DagCborCidGeneric<64>;

impl<'de, const S: usize> serde::Deserialize<'de> for DagCborCidGeneric<S> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut r = std::io::Cursor::new(
            ciborium::tag::Required::<Vec<u8>, 42>::deserialize(deserializer)?.0,
        );
        let mut prefix = [0u8; 1];
        r.read_exact(&mut prefix[..])
            .map_err(serde::de::Error::custom)?;
        let [prefix] = prefix;
        if prefix != 0x00 {
            return Err(serde::de::Error::invalid_value(
                serde::de::Unexpected::Unsigned(prefix as u64),
                &"expected multibase identity (0x00) prefix",
            ));
        }
        Ok(DagCborCidGeneric(
            cid::CidGeneric::<S>::read_bytes(r).map_err(serde::de::Error::custom)?,
        ))
    }
}

impl<const S: usize> From<cid::CidGeneric<S>> for DagCborCidGeneric<S> {
    fn from(value: cid::CidGeneric<S>) -> Self {
        Self(value)
    }
}

impl<const S: usize> From<DagCborCidGeneric<S>> for cid::CidGeneric<S> {
    fn from(value: DagCborCidGeneric<S>) -> Self {
        value.0
    }
}

impl<'a, const S: usize> From<&'a DagCborCidGeneric<S>> for &'a cid::CidGeneric<S> {
    fn from(value: &'a DagCborCidGeneric<S>) -> Self {
        &value.0
    }
}
