use tokio::io::{
    stdin,
    AsyncBufReadExt,
    BufReader,
    ErrorKind,
    Interest,
    Stdin
};
use tokio::net::TcpStream;

use std::error::Error;

const BUFFER_SIZE: usize = 16384;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let stdin: Stdin = stdin();
    let buffer: BufReader<Stdin> = BufReader::new(stdin);
    let mut lines = buffer.lines();
    let client = TcpStream::connect(("127.0.0.1".to_string(), 8080)).await?;
    let mut buffer: [u8; BUFFER_SIZE] = [0; BUFFER_SIZE];
    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                result = lines.next_line() => {
                    if let Ok(Some(buffer)) = result {
                        let buffer: String = buffer + "\n";
                        match client.ready(Interest::WRITABLE).await {
                            Ok(_) => {
                                match client.try_write(buffer.as_bytes()) {
                                    Err(ref e) if e.kind() == ErrorKind::WouldBlock => continue,
                                    Ok(n) => if n == 0 {
                                        break;
                                    },
                                    Err(e) => println!("{e}")
                                }
                            },
                            Err(e) => println!("{e}")
                        }
                    }
                }
                _ = client.readable() => {
                     match client.try_read(&mut buffer) {
                          Err(ref e) if e.kind() == ErrorKind::WouldBlock => continue,
                          Ok(n) => if n == 0 {
                              break;
                          } else {
                             let text = String::from_utf8(buffer[..n].to_vec()).unwrap();
                             print!("{text}");
                         },
                          Err(e) => println!("{e}")
                     }
                }
            }
        }
    });
    let _ = handle.await;
    Ok(())
}
