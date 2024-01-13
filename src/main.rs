#![feature(extract_if)]
use std::time::Duration;

use futures::TryFutureExt;
use tokio::task;

const CHUNK_SIZE: u32 = 16 * 1024;

#[tokio::main]
async fn main() {
    console_subscriber::init();

    let client = reqwest::Client::builder().http1_only().build().unwrap();
    let mut futs = Vec::new();
    for idx in 0..10000 {
        let start = CHUNK_SIZE * idx;
        let end = start + CHUNK_SIZE;
        let range = format!("bytes={}-{}", start, end);
        let fut = client
        .get("https://storage.googleapis.com/weston-public-test/weston/data/d3144d84-b8e5-4b9a-bc19-9ca64dabe0fd.lance")
        .header("Range", range)
        .send().and_then(|rsp| rsp.bytes().and_then(|bytes| std::future::ready(Ok(bytes.len()))));
        futs.push(task::spawn(fut));
    }

    let mut counter = 0;
    while !futs.is_empty() {
        let complete = futs.extract_if(|fut| fut.is_finished()).collect::<Vec<_>>();
        if complete.is_empty() {
            println!("No results, backing off for 1 second");
            tokio::time::sleep(Duration::from_secs(1)).await;
        } else {
            for fut in complete {
                counter += 1;
                match fut.await.unwrap() {
                    Ok(bytes_received) => {
                        println!("{}: Downloaded {} bytes", counter, bytes_received);
                    }
                    Err(err) => {
                        println!("{}: Request failed: {}", counter, err);
                    }
                }
            }
        }
    }
}
