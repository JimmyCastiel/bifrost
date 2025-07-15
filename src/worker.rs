use thiserror::Error as ThisError;

use tokio::net::TcpStream;
use tokio::io::{AsyncRead, AsyncWrite, Interest};

use std::io::ErrorKind;

#[derive(ThisError, Default, Debug)]
pub(crate) enum WorkerError {
    #[default]
    #[error("Unknown worker error.")]
    Unknown,
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
    backend_socket: S
}

impl Worker<TcpStream> {
    pub(crate) fn new(client_socket: TcpStream, backend_socket: TcpStream) -> Self {
        Worker {
            client_socket,
            backend_socket
        }
    }

    pub(crate) async fn start(self) {
        let mut buffer: [u8; BUFFER_SIZE] = [0; BUFFER_SIZE];
        loop {
            let read = self.read(&self.client_socket, &mut buffer).await;
            match read {
                Ok(n) => {
                    let _ = self.write(&self.backend_socket, &buffer[..n]).await;
                    let resp = self.read(&self.backend_socket, &mut buffer).await;
                    if let Ok(n) = resp {
                        let _ = self.write(&self.client_socket, &buffer[..n]).await;
                    }
                },
                Err(e) => {
                    if let WorkerError::EmptyRead = e {
                        break;
                    }
                }
            }
        }
        let _ = self.write(&self.client_socket, "Thank you for connecting.\n".as_bytes()).await;
    }

    async fn read(&self, socket: &TcpStream, buffer: &mut [u8]) -> WorkerResult {
        match self.client_socket.ready(Interest::READABLE).await {
            Ok(ref ready) if ready.is_readable() => {
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
            },
            Ok(_) => return Err(WorkerError::Continue),
            Err(e) => println!("{e}"),
        }
        Err(WorkerError::Continue)
    }

    async fn write(&self, socket: &TcpStream, buffer: &[u8]) -> WorkerResult {
        match self.client_socket.ready(Interest::WRITABLE).await {
            Ok(ref ready) if ready.is_writable() => {
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
            },
            Ok(_) => return Err(WorkerError::Continue),
            Err(e) => println!("{e}")
        }
        Err(WorkerError::Continue)
    }
}

