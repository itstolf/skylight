fn main() {
    let rq = atrium_xrpc::client::reqwest::ReqwestClient::new("https://bsky.app".to_string());
    println!("Hello, world!");
}
