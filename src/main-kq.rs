mod event;
mod signal;

use crate::{
    event::EventType,
    signal::{
        disable_signals,
        get_signals_kevent,
        SignalType
    }
};

use std::{
    collections::BTreeMap,
    error::Error,
    io::{
        Read,
        Write
    },
    net::{
        SocketAddr,
        TcpListener,
        TcpStream
    },
    os::fd::AsRawFd,
    process::exit,
    ptr::null,
};

use libc::{
    kevent,
    kqueue,
    EV_EOF
};
use nix::errno::Errno;


fn main() -> Result<(), Box<dyn Error>> {
    disable_signals();

    let mut sockets: BTreeMap<i32, (TcpStream, SocketAddr)> = BTreeMap::new();
    let stream = TcpListener::bind("127.0.0.1:8000")?;
    eprintln!("Listening to port {}", stream.local_addr()?);
    stream.set_nonblocking(true)?;
    let fd: i32 = stream.as_raw_fd();
    
    let kq: i32 = unsafe { kqueue() };
    if kq == -1 {
        let errno = Errno::last();
        eprintln!("Event queue couldn't be registered with the kernel due to error {errno}");
        exit(1);
    }

    let mut changes: Vec<kevent> = vec![EventType::ReadFd.get_add_event(fd as usize)?];
    changes.extend(get_signals_kevent());

    let mut events: Vec<kevent> = Vec::with_capacity(15);
    'event_loop: loop {
        let n: i32 = unsafe { kevent(kq, changes.as_ptr(), changes.len() as i32, events.as_mut_ptr(), events.capacity() as i32, null()) };

        if n == -1 {
            let errno = Errno::last();
            eprintln!("No events could be received due to error {errno}");
            //unsafe { events.set_len(0) };
        } else {
            changes.clear();
            unsafe { events.set_len(n as usize) };
        }

        let mut buf: [u8; 128] = [0; 128];
        for ev in &events[0..n as usize] {
            match ev.filter.into() {
                EventType::ReadFd => {
                    if ev.ident == fd as usize {
                        let mut n: usize = ev.data as usize;
                        while n > 0 {
                            if let Ok(s) = stream.accept() {
                                let _ = s.0.set_nonblocking(true);
                                eprintln!("Client {} connected", s.1);

                                changes.push(EventType::ReadFd.get_add_event(s.0.as_raw_fd() as usize)?);
                                sockets.insert(s.0.as_raw_fd(), s);

                                n -= 1;
                            }
                        }
                    } else if let Some(s) = sockets.get_mut(&(ev.ident as i32)) {
                        if ev.flags & EV_EOF != 0 {
                            eprintln!("Client {} disconnected", s.1);
                            changes.push(EventType::ReadFd.get_del_event(s.0.as_raw_fd() as usize)?);
                            sockets.remove(&(ev.ident as i32));
                        } else {
                            let _ = s.0.read(&mut buf);
                            let _ = s.0.write(&buf);
                        }
                    }
                },
                EventType::Signal => {
                    match (ev.ident as i32).into() {
                        SignalType::Shutdown => {
                            eprintln!("Shutting down");
                            break 'event_loop;
                        },
                        _ => eprintln!("Unknown signal received")
                    }
                }
            }
        }
    }

    Ok(())
}
