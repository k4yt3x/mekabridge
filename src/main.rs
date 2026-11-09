//! Binary entry point. All behaviour lives in the library so integration tests can drive the same
//! code paths without spawning a process.

use std::process::ExitCode;

use clap::Parser;
use meka_bridge::cli::Cli;

fn main() -> ExitCode {
    // Set here rather than inside `run`, because `--help` and `--version` print before `run` is
    // reached. `meka_bridge::bridge::run` takes it back for the daemon, which needs the opposite.
    #[cfg(unix)]
    meka_bridge::cli::exit_quietly_on_broken_pipe();

    let cli = Cli::parse();
    match cli.run() {
        Ok(code) => code,
        Err(error) => {
            // Configuration failures happen before the tracing subscriber exists, so the error is
            // printed here rather than logged. The source chain carries the actionable detail.
            eprintln!("meka-bridge: {error}");
            let mut source = std::error::Error::source(&error);
            while let Some(inner) = source {
                eprintln!("  caused by: {inner}");
                source = inner.source();
            }
            ExitCode::FAILURE
        }
    }
}
