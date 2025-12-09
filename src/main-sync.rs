mod listener;
mod backend;
mod worker;

use polling::{
    Event,
    Events,
    Poller,
};

use num_derive::FromPrimitive;
use num_traits::FromPrimitive;

use std::{
    collections::VecDeque,
    error::Error,
    io::ErrorKind,
    net::{
        Shutdown,
        SocketAddr,
        TcpListener,
        TcpStream
    }, sync::{
        atomic::{
            AtomicBool,
            Ordering
        },
        Arc}, time::Duration
};

#[derive(Debug, PartialEq, FromPrimitive)]
enum EventKeys {
    Listener,
    Socket
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut sockets: VecDeque<(TcpStream, SocketAddr)> = VecDeque::new();
    let stop = Arc::new(AtomicBool::new(false));
    let poller = Arc::new(Poller::new()?);
    let socket = Arc::new(TcpListener::bind("127.0.0.1:8000")?);
    socket.set_nonblocking(true)?;

    unsafe {
        poller.add(&socket, Event::readable(EventKeys::Listener as usize))?;
    }

    let p = Arc::clone(&poller);
    let st = Arc::clone(&stop);
    let _ = ctrlc::set_handler(move || {
        println!("Signal received !");
        st.store(true, Ordering::Relaxed);
        let _ = p.notify();
    });

    let mut events = Events::new();
    loop {
        // Wait for at least one I/O event.
        events.clear();
        let count = poller.wait(&mut events, Some(Duration::new(1, 0)))?;
        //println!("c: {count}, s: {sockets:?}, p: {poller:?}");

        if stop.load(Ordering::Relaxed) {
            break;
        }

        if count == 0 {
            println!("{sockets:?}");
            for _ in 0..sockets.len() {
                let s = sockets.pop_front();
                if let Some(s) = s {
                    let mut buf = [0;1];
                    let r = s.0.peek(&mut buf);
                    if let Ok(c) = r &&
                        c == 0 {
                        let _ = poller.delete(&s.0);
                        //s.0.shutdown(Shutdown::Both).unwrap();
                        continue;
                    } else if let Err(e) = r {
                        println!("e: {e:?}");
                        if e.kind() == ErrorKind::ConnectionReset {
                            let _ = poller.delete(&s.0);
                        }
                    }
                    sockets.push_back(s);
                }
            }
        } else {
            for ev in events.iter() {
                let key: EventKeys = FromPrimitive::from_usize(ev.key).unwrap();
                match key {
                    EventKeys::Listener => {
                        // Perform a non-blocking accept operation.
                        let so = socket.accept()?;
                        let _ = poller.delete(&so.0);
                        unsafe {
                            poller.add(&so.0, Event::readable(EventKeys::Socket as usize))?;
                        }
                        sockets.push_back(so);
                        // Set interest in the next readability event.
                        poller.modify(&socket, Event::readable(EventKeys::Listener as usize))?;
                    },
                    EventKeys::Socket => {}
                }
            }
        }
    }

    Ok(())
}

