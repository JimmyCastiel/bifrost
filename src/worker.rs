use crate::listener::Runnable;

use thiserror::Error as ThisError;

use tokio::net::{
    TcpStream,
    tcp::{
        OwnedReadHalf,
        OwnedWriteHalf
    }
};
use tokio::io::{
    AsyncRead,
    AsyncWrite,
};
use tokio_util::task::TaskTracker;

use std::io::{
    Error as IoError,
    ErrorKind
};

#[derive(ThisError, Default, Debug)]
pub(crate) enum WorkerError {
    #[default]
    #[error("Unrecoverable worker error.")]
    Unrecoverable,
    #[error("An IO error occured.")]
    IoError(#[from] IoError),
    #[error("Empty read.")]
    EmptyRead,
    #[error("Empty write.")]
    EmptyWrite,
    #[error("Skip iteration.")]
    Continue,
}

type WorkerResult = Result<usize, WorkerError>;

const BUFFER_SIZE: usize = 16384;

pub(crate) struct Worker<S: AsyncRead + AsyncWrite> {
    client_socket: S,
    backend_socket: S,
}

impl Worker<TcpStream> {
    pub(crate) fn new(client_socket: TcpStream, backend_socket: TcpStream) -> Self {
        Worker {
            client_socket,
            backend_socket,
        }
    }

    async fn read(socket: &OwnedReadHalf, buffer: &mut [u8]) -> WorkerResult {
        match socket.try_read(buffer) {
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => { return Err(WorkerError::Continue); },
            Ok(n) => {
                if n == 0 {
                    return Err(WorkerError::EmptyRead);
                }
                return Ok(n);
            },
            Err(e) => println!("{e}")
        }
        Err(WorkerError::Unrecoverable)
    }

    async fn write(socket: &OwnedWriteHalf, buffer: &[u8]) -> WorkerResult {
        match socket.try_write(buffer) {
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => { return Err(WorkerError::Continue); },
            Ok(n) => {
                if n == 0 {
                    return Err(WorkerError::EmptyWrite);
                }
                return Ok(n);
            },
            Err(e) => println!("{e}")
        }
        Err(WorkerError::Unrecoverable)
    }

    async fn exec(read: OwnedReadHalf, write: OwnedWriteHalf) -> Option<WorkerError> {
        let mut client_buffer: [u8; BUFFER_SIZE] = [0; BUFFER_SIZE];
        loop {
            if (read.readable().await).is_ok() {
                match Worker::read(&read, &mut client_buffer).await {
                    Ok(n) => {
                        match Worker::write(&write, &client_buffer[..n]).await {
                            Ok(n) => {
                                println!("{n}");
                            },
                            Err(e) => {
                                println!("{e:?}");
                                if let WorkerError::EmptyWrite = e {
                                    break None;
                                } else if let WorkerError::Unrecoverable = e {
                                    break Some(e);
                                }
                            }
                        }
                    },
                    Err(e) => {
                        println!("{e:?}");
                        if let WorkerError::EmptyRead = e {
                            break None;
                        } else if let WorkerError::Unrecoverable = e {
                            break Some(e);
                        }
                    }
                }
            }
        }

    }
}

impl Runnable for Worker<TcpStream> {
    #[allow(refining_impl_trait)]
    async fn run(self) -> Result<(), WorkerError> {
        let (client_read, client_write): (OwnedReadHalf, OwnedWriteHalf) = self.client_socket.into_split();
        let (backend_read, backend_write): (OwnedReadHalf, OwnedWriteHalf) = self.backend_socket.into_split();
        let tracker = TaskTracker::new();
        tracker.spawn(Worker::exec(client_read, backend_write));
        tracker.spawn(Worker::exec(backend_read, client_write));
        tracker.close();
        tracker.wait().await;    
        Ok(())
    }
}
