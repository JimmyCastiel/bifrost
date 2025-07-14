use crate::worker::Worker;

use thiserror::Error as ThisError;

use tokio::net::{ TcpListener, ToSocketAddrs };

use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use std::io::Error as IoError;

#[derive(ThisError, Default, Debug)]
pub(crate) enum ServerError {
    #[default]
    #[error("Unknown server error.")]
    Unknown,
    #[error("An IO error occured.")]
    IoError(#[from] IoError)
}

#[derive(Debug)]
pub(crate) struct Server<T> {
    token: CancellationToken,
    listener: T,
    tracker: TaskTracker
}

impl Server<TcpListener> {
    pub(crate) async fn new<B: ToSocketAddrs>(token: CancellationToken, bind_addr: B) -> Result<Self, ServerError> {
        Ok(Self {
            token,
            listener: TcpListener::bind(bind_addr).await?,
            tracker: TaskTracker::new()
        })
    }

    pub(crate) async fn start(self) -> Result<(), ServerError> {
        if self.tracker.is_closed() {
            self.tracker.reopen();
        }
        self.run().await;
        Ok(())
    }

    async fn run(self) {
        loop {
            tokio::select! {
                res = self.listener.accept() => {
                    match res {
                        Ok((socket, _)) => {
                            println!("{socket:?}");
                            self.tracker.spawn(Worker::new(socket).start());
                        },
                        Err(e) => println!("{e}")
                    }
                }
                _ = self.token.cancelled() => {
                    self.stop().await;
                    break;
                }
            }
        }
    }

    pub(crate) async fn stop(self) {
        drop(self.listener);
        self.tracker.close();
        self.tracker.wait().await;
    }
}

