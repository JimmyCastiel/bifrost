use crate::io::polling::{
    MAX_EVENTS,
    Events,
    Pollable,
    Poller,
    PollingResult
};

use std::{
    convert::From,
    error::Error,
    fmt::{
        Display,
        Formatter,
        Result
    },
    ptr::{
        null,
        null_mut
    },
    time::Duration
};

use libc::{
    kevent, kqueue, EVFILT_READ, EVFILT_SIGNAL, EVFILT_WRITE, EV_ADD, EV_EOF
};

#[derive(Debug)]
pub(crate) enum BsdPollerError {
    Init,
    BadFd
}

impl Display for BsdPollerError {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(f, "Error",)
    }
}

impl Error for BsdPollerError {}

pub(crate) struct BsdPoller {
    changes: Events,
    events: Events,
    kq: i32
}

#[repr(transparent)]
#[derive(Clone, Debug)]
pub(crate) struct Event(kevent);

impl Event {
    pub(crate) fn is_signal(&self) -> bool {
        self.0.filter == EVFILT_SIGNAL
    }

    pub(crate) fn is_fd(&self) -> bool {
        self.0.filter == EVFILT_READ ||
        self.0.filter == EVFILT_WRITE
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.0.flags & EV_EOF == EV_EOF
    }
}

impl From<kevent> for Event {
    fn from(event: kevent) -> Self {
        Event(event)
    }
}

impl From<&Event> for usize {
    fn from(event: &Event) -> Self {
        event.0.ident
    }
}

impl From<usize> for Event {
    fn from(fd: usize) -> Self {
        Event(kevent{
            ident: fd,
            filter: EVFILT_READ,
            flags: EV_ADD + EV_EOF,
            fflags: 0,
            data: 0,
            udata: null_mut()
        })
    }
}

impl Pollable for BsdPoller {
    fn new() -> Self{
        let kq: i32 = unsafe { kqueue() };

        Self {
            changes: Vec::with_capacity(MAX_EVENTS),
            events: Vec::with_capacity(MAX_EVENTS),
            kq
        }
    }

    fn add_event(&mut self, event: Event) -> PollingResult<()> {
        self.changes.push(event);
        Ok(())
    }

    fn poll(&mut self, _timeout: Option<Duration>) -> PollingResult<Events> {
        if self.kq == -1 {
            return Err(BsdPollerError::Init);
        }

        let n: i32 = unsafe { kevent(self.kq, self.changes.as_ptr() as *const kevent, self.changes.len() as i32, self.events.as_mut_ptr() as *mut kevent, MAX_EVENTS as i32, null()) };
        unsafe {
            self.changes.set_len(0);
            self.events.set_len(n as usize);
        }

        Ok(self.events.clone())
    }
}

impl From<&Poller> for i32 {
    fn from(poller: &Poller) -> Self {
        poller.kq
    }
}
