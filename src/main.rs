mod server;
mod worker;

use crate::server::Server;

use tokio::signal;

use tokio_util::task::TaskTracker;
use tokio_util::sync::CancellationToken;

use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let token: CancellationToken = CancellationToken::new();
    let tracker: TaskTracker = TaskTracker::new();
    let server = Server::new(token.clone(), ("127.0.0.1".to_string(), 8080)).await?;
    println!("{server:?}");
    tracker.spawn(server.start());
    tracker.close();
    loop {
        tokio::select! {
            _ = tracker.wait() => { break }
            _ = signal::ctrl_c() => {token.cancel()}
        }
    }
    Ok(())
}
