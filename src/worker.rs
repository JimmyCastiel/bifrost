use crate::listener::Runnable;

use thiserror::Error as ThisError;

use tokio::net::TcpStream;
use tokio::io::{AsyncRead, AsyncWrite, Interest};
use tokio::select;

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

const BUFFER_SIZE: usize = 8192;

pub(crate) struct Worker<S: AsyncRead + AsyncWrite> {
    client_socket: S,
    backend_socket: S
}

impl Worker<TcpStream> {
    pub(crate) fn new(client_socket: TcpStream, backend_socket: TcpStream) -> Self {
        Worker {
            client_socket,
            backend_socket
        }
    }

    async fn read(&self, socket: &TcpStream, buffer: &mut [u8]) -> WorkerResult {
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

    async fn write(&self, socket: &TcpStream, buffer: &[u8]) -> WorkerResult {
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
}

impl Runnable for Worker<TcpStream> {
    #[allow(refining_impl_trait)]
    async fn run(self) -> Result<(), WorkerError> {
        let mut buffer: [u8; BUFFER_SIZE] = [0; BUFFER_SIZE];
        loop {
            select! {
                _ = self.client_socket.readable() => {
                    match self.read(&self.client_socket, &mut buffer).await {
                        Ok(n) => {
                            let r = self.write(&self.backend_socket, &buffer[..n]).await;
                            println!("{r:?}");
                        },
                        Err(e) => {
                            println!("{e:?}");
                            if let WorkerError::EmptyRead = e {
                                break;
                            } else if let WorkerError::Unrecoverable = e {
                                break;
                            }
                        }
                    }
                }
                _ = self.backend_socket.readable() => {
                    match self.read(&self.backend_socket, &mut buffer).await {
                        Ok(n) => {
                            let r = self.write(&self.client_socket, &buffer[..n]).await;
                            println!("{r:?}");
                        },
                        Err(e) => {
                            println!("{e:?}");
                            if let WorkerError::EmptyRead = e {
                                break;
                            } else if let WorkerError::Unrecoverable = e {
                                break;
                            }
                        }
                    }

                }
            }
            let client_ready = self.client_socket.ready(Interest::READABLE | Interest::WRITABLE).await?;
            let backend_ready = self.backend_socket.ready(Interest::READABLE | Interest::WRITABLE).await?;
            if client_ready.is_read_closed()
                && client_ready.is_write_closed() 
                && backend_ready.is_read_closed()
                && backend_ready.is_write_closed() {
                break;
            }
        }
        //let _ = self.write(&self.client_socket, "Thank you for connecting.\n".as_bytes()).await;
        Ok(())
    }
}
