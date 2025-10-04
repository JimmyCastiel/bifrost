use crate::worker::Worker;

use std::error::Error;
use std::io::Error as IoError;

use thiserror::Error as ThisError;

use tokio::net::{ TcpListener, TcpStream, ToSocketAddrs };

use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

#[derive(ThisError, Default, Debug)]
pub(crate) enum ListenerError {
    #[default]
    #[error("Unknown server error.")]
    Unknown,
    #[error("An IO error occured with message : {}.", self.source().unwrap_or(&Self::Unknown))]
    IoError(#[from] IoError),
    #[error("Server was shutdown, please instanciate a new instance.")]
    Unusable
}

pub(crate) trait Runnable {
    async fn run(self) -> Result<(), impl Error>;
}

pub type ListenerResult = Result<(), ListenerError>;

#[derive(Debug)]
pub(crate) struct Listener<T> {
    shutdown_token: CancellationToken,
    listener: T,
    tracker: TaskTracker
}

impl Listener<TcpListener> {
    pub(crate) async fn new<B: ToSocketAddrs>(shutdown_token: CancellationToken, bind_addr: B) -> Result<Self, ListenerError> {
        Ok(Self {
            shutdown_token,
            listener: TcpListener::bind(bind_addr).await?,
            tracker: TaskTracker::new()
        })
    }
}

impl Runnable for Listener<TcpListener> {
    #[allow(refining_impl_trait)]
    async fn run(self) -> ListenerResult {
        if self.tracker.is_closed() {
            return Err(ListenerError::Unusable);
        }
        loop {
            tokio::select! {
                res = self.listener.accept() => {
                    match res {
                        Ok((client_socket, _)) => {
                            println!("{client_socket:?}");
                            let backend_socket: TcpStream = TcpStream::connect("www.google.fr:80").await?;
                            self.tracker.spawn(Worker::new(client_socket, backend_socket).run());
                        },
                        Err(e) => println!("{e}")
                    }
                }
                _ = self.shutdown_token.cancelled() => {
                    drop(self.listener);
                    break;
                }
            }
        }
        self.tracker.close();
        self.tracker.wait().await;
        Ok(())
    }
}

