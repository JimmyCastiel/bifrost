use std::ptr::null_mut;

use libc::{
    kevent,
    sigaction,
    EVFILT_SIGNAL,
    EV_ADD,
    SIGINT,
    SIGUSR1,
    SIGUSR2,
    SIG_IGN
};

pub(crate) enum SignalType {
    Shutdown,
    Reload,
    Undefined
}

impl From<i32> for SignalType {
    fn from(value: i32) -> Self {
        match value {
            SIGINT => SignalType::Shutdown,
            SIGUSR2 => SignalType::Reload,
            _ => SignalType::Undefined
        }
    }
}

fn disable_signal(signal: i32) {
    let act: sigaction = sigaction {
        sa_sigaction: SIG_IGN,
        sa_mask: 0,
        sa_flags: 0
    };
    unsafe { sigaction(signal, &act, null_mut()) };
}

pub(crate) fn disable_signals() {
    for sig in [SIGINT, SIGUSR1, SIGUSR2] {
        disable_signal(sig);
    }
}

pub(crate) fn get_signals_kevent() -> Vec<kevent> {
    [SIGINT, SIGUSR1, SIGUSR2]
        .iter()
        .map(|sig| kevent { 
            ident: *sig as usize,
            filter: EVFILT_SIGNAL,
            flags: EV_ADD,
            fflags: 0,
            data: 0,
            udata: null_mut()
        })
        .collect()
}

