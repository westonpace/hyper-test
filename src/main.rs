#![feature(extract_if)]

use std::time::Duration;

use futures::{stream, StreamExt, TryFutureExt};
use tokio::time::sleep;

const CHUNK_SIZE: u32 = 16 * 1024;

#[tokio::main]
async fn main() {
    console_subscriber::init();

    let client = reqwest::Client::builder().http1_only().build().unwrap();
    let mut parent_tasks = Vec::with_capacity(128);
    for thread_idx in 0..128 {
        let client = &client;
        parent_tasks.push( async move {
            let mut inner_tasks = Vec::with_capacity(128);
            sleep(Duration::from_millis(1)).await;
            for run_idx in 0..128 {
                let idx = thread_idx * 128 + run_idx;
                let start = CHUNK_SIZE * idx;
                let end = start + CHUNK_SIZE;
                let range = format!("bytes={}-{}", start, end);
                let fut = 
                    client
                    .get("https://storage.googleapis.com/weston-public-test/weston/data/d3144d84-b8e5-4b9a-bc19-9ca64dabe0fd.lance")
                    .header("Range", range).send().and_then(|rsp| rsp.bytes().and_then(|bytes| std::future::ready(Ok(bytes.len()))));
                inner_tasks.push(fut);
            }
            inner_tasks
        });
    }

    let mut counter = 0;
    let mut stream = stream::iter(parent_tasks)
        .buffered(8)
        .flat_map(|item| stream::iter(item))
        .buffered(8);
    while let Some(res) = stream.next().await {
        counter += 1;
        match res {
            Ok(bytes_received) => {
                println!("{}: Downloaded {} bytes", counter, bytes_received);
            }
            Err(err) => {
                println!("{}: Request failed: {}", counter, err);
            }
        }
    }
}
