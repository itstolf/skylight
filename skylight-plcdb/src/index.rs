pub fn split_key(k: &[u8]) -> Option<Vec<String>> {
    k.split(|c| *c == '\0' as u8)
        .map(|v| String::from_utf8(v.to_vec()).ok())
        .collect()
}

pub fn make_key(parts: &[&str]) -> Vec<u8> {
    parts
        .iter()
        .map(|p| p.as_bytes())
        .collect::<Vec<_>>()
        .join(&b"\0"[..])
}

pub fn make_key_prefix(parts: &[&str]) -> Vec<u8> {
    let mut prefix = make_key(parts);
    prefix.push('\0' as u8);
    prefix
}

pub type Index = heed::Database<heed::types::CowSlice<u8>, heed::types::Unit>;
