use std::{
    time::Duration,
    vec::Vec
};

#[cfg(any(target_os = "freebsd", target_os = "netbsd", target_os = "openbsd", target_os = "macos"))]
mod bsd;

use crate::io::polling::bsd::BsdPollerError;

const MAX_EVENTS: usize = 15;

#[cfg(any(target_os = "freebsd", target_os = "netbsd", target_os = "openbsd", target_os = "macos"))]
pub(crate) type Event = bsd::Event;

pub(crate) type Events = Vec<Event>;

#[cfg(any(target_os = "freebsd", target_os = "netbsd", target_os = "openbsd", target_os = "macos"))]
pub(crate) type PollingResult<T> = Result<T, BsdPollerError>;

pub(crate) trait Pollable {
    fn new() -> Self;
    fn add_event(&mut self, event: Event) -> PollingResult<()>;
    fn poll(&mut self, timeout: Option<Duration>) -> PollingResult<Events>;
}

#[cfg(any(target_os = "freebsd", target_os = "netbsd", target_os = "openbsd", target_os = "macos"))]
pub(crate) type Poller = bsd::BsdPoller;

