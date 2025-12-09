use std::ptr::null_mut;

use libc::{
    kevent,
    EVFILT_READ,
    EVFILT_SIGNAL,
    EV_ADD,
    EV_DELETE,
    EV_EOF
};
use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub(crate) enum EventError {
    #[error("Undefined event type")]
    Undefined
}

pub(crate) type EventResult<T> = Result<T, EventError>;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub(crate) enum EventType {
    ReadFd,
    Signal
}

impl From<&EventType> for i16 {
    fn from(evt: &EventType) -> i16 {
        match evt {
            EventType::ReadFd => EVFILT_READ,
            EventType::Signal => EVFILT_SIGNAL
        } 
    }
}

impl From<i16> for EventType {
    fn from(value: i16) -> Self {
        match value {
            EVFILT_READ => EventType::ReadFd,
            EVFILT_SIGNAL => EventType::Signal,
            _ => EventType::ReadFd
        }
    }
}

impl EventType {
    fn get_filters(&self) -> u16 {
        match self {
            EventType::ReadFd => EV_ADD | EV_EOF,
            EventType::Signal => EV_ADD
        } 
    }

    pub(crate) fn get_add_event(&self, ident: usize) -> EventResult<kevent> {
        match self {
            &EventType::ReadFd => Ok(kevent {
                ident,
                filter: self.into(),
                flags: self.get_filters(),
                fflags: 0,
                data: 0,
                udata: null_mut()
            }),
            _ => Err(EventError::Undefined)
        }
    }

    pub(crate) fn get_del_event(&self, ident: usize) -> EventResult<kevent> {
        match self {
            &EventType::ReadFd => Ok(kevent {
                ident,
                filter: self.into(),
                flags: EV_DELETE,
                fflags: 0,
                data: 0,
                udata: null_mut()
            }),
            _ => Err(EventError::Undefined)
        }
    }
}

