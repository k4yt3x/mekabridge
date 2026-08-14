//! Binary entry point. All behaviour lives in the library so integration tests can drive the same
//! code paths without spawning a process.

use std::process::ExitCode;

use clap::Parser;
use mekabridge::cli::Cli;

/// Restore the default `SIGPIPE` disposition.
///
/// Rust sets `SIGPIPE` to `SIG_IGN` during startup, which turns a perfectly ordinary
/// `mekabridge conversations list | head` into a panic on the first write past the closed pipe. The
/// operator subcommands exist to be piped into `grep` and `head`, so the default disposition (exit
/// quietly) is the correct one here.
#[cfg(unix)]
fn restore_sigpipe() {
    // SAFETY: called before any other thread is spawned, with a valid signal number and the
    // libc-provided default handler. `signal` returns the previous handler, which is not needed.
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }
}

fn main() -> ExitCode {
    #[cfg(unix)]
    restore_sigpipe();

    let cli = Cli::parse();
    match cli.run() {
        Ok(code) => code,
        Err(error) => {
            // Configuration failures happen before the tracing subscriber exists, so the error is
            // printed here rather than logged. The source chain carries the actionable detail.
            eprintln!("mekabridge: {error}");
            let mut source = std::error::Error::source(&error);
            while let Some(inner) = source {
                eprintln!("  caused by: {inner}");
                source = inner.source();
            }
            ExitCode::FAILURE
        }
    }
}
