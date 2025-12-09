mod event;
mod signal;
mod io;

use crate::io::polling::{
    Events,
    Pollable,
    Poller,
};

use crate::{
    event::EventType,
    signal::{
        disable_signals,
        get_signals_kevent,
    }
};

use std::{
    collections::BTreeMap,
    error::Error,
    net::{
        SocketAddr,
        TcpListener,
        TcpStream
    },
    os::fd::AsRawFd,
};

use libc::kevent;


fn main() -> Result<(), Box<dyn Error>> {
    disable_signals();
    let mut poller: Poller = Poller::new();

    let mut sockets: BTreeMap<i32, (TcpStream, SocketAddr)> = BTreeMap::new();
    let stream = TcpListener::bind("127.0.0.1:8000")?;
    eprintln!("Listening to port {}", stream.local_addr()?);
    stream.set_nonblocking(true)?;
    let fd: i32 = stream.as_raw_fd();
    
    let kevt: kevent = EventType::ReadFd.get_add_event(fd as usize)?;
    let mut changes: Vec<kevent> = vec![kevt];
    changes.extend(get_signals_kevent());
    let _ = poller.add_event(kevt.into());
    for s in changes.into_iter() {
        let _ = poller.add_event(s.into());
    }
    'event_loop: loop {
        let events: Events = poller.poll(None)?;

        for event in events {
            if event.is_signal() {
                break 'event_loop;
            } else if event.is_fd() {
                let i: usize = (&event).into();
                if i as i32 == fd {
                    let socket : (TcpStream, SocketAddr) = stream.accept()?;
                    println!("connection from {}", socket.1);
                    let i: usize = socket.0.as_raw_fd() as usize;
                    let _ = poller.add_event(i.into());
                    sockets.insert(i as i32, socket);
                } else if event.is_closed() {
                    let socket = sockets.get(&(i as i32)).unwrap();
                    println!("connection closed {}", socket.1);
                    sockets.remove(&(i as i32));
                }
            }
        }
    }

    Ok(())
}
