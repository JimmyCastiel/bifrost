use tokio::net::TcpStream;
use tokio::io::{AsyncRead, AsyncWrite, Interest};

use std::io::ErrorKind;
use std::str::from_utf8;

const BUFFER_SIZE: usize = 4096;

pub(crate) struct Worker<S: AsyncRead + AsyncWrite> {
    socket: S
}

impl Worker<TcpStream> {
    pub(crate) fn new(socket: TcpStream) -> Self {
        Worker {
            socket
        }
    }

    pub(crate) async fn start(self) {
        loop {
            let mut buffer: [u8; BUFFER_SIZE] = [0; BUFFER_SIZE];
            let n = self.read(&mut buffer).await;
            if n == 0 {
                break;
            } else if n > 0 {
                if let Ok(n) = n.try_into() {
                    let payload = from_utf8(&buffer[..n]);
                    match payload {
                        Ok(payload) => print!("{payload}"),
                        _ => continue
                    }
                }
            }
        }
        let _ = self.write("Thank you for connecting.\n".as_bytes()).await;
    }

    async fn read(&self, buffer: &mut [u8]) -> isize {
        match self.socket.ready(Interest::READABLE).await {
            Ok(ref ready) if ready.is_readable() => {
                match self.socket.try_read(buffer) {
                    Err(ref e) if e.kind() == ErrorKind::WouldBlock => { return -2; },
                    Ok(n) => {
                        return n as isize;
                    },
                    Err(e) => {
                        println!("{e}");
                    }
                }
            },
            Ok(_) => return -3,
            Err(e) => {
                println!("{e}");
            }
        }
        -1
    }

    async fn write(&self, buffer: &[u8]) -> isize {
        match self.socket.ready(Interest::WRITABLE).await {
            Ok(ref ready) if ready.is_writable() => {
                match self.socket.try_write(buffer) {
                    Err(ref e) if e.kind() == ErrorKind::WouldBlock => { return -2; },
                    Ok(n) => {
                        return n as isize;
                    },
                    Err(e) => {
                        println!("{e}");
                    }
                }
            },
            Ok(_) => return -3,
            Err(e) => {
                println!("{e}");
            }
        }
        -1
    }
}

