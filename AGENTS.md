# AGENTS.md

This file provides guidance to AI agents when working with code in this repository.

## Rust Coding Guidelines

- Prioritize code correctness and clarity. Speed and efficiency are secondary priorities unless otherwise specified.
- Do not write organizational or comments that summarize the code. Comments should only be written in order to explain "why" the code is written in some way in the case there is a reason that is tricky / non-obvious.
- Never hand-wrap comments. Write each comment (and doc comment) as one line per paragraph and let `cargo +nightly fmt` wrap it (`.rustfmt.toml` has `wrap_comments = true`). If fmt's wrap lands awkwardly, reword the prose rather than inserting a manual break.
- Prefer implementing functionality in existing files unless it is a new logical component. Avoid creating many small files.
- Avoid using functions that panic like `unwrap()`, instead use mechanisms like `?` to propagate errors.
- Be careful with operations like indexing which may panic if the indexes are out of bounds.
- Never silently discard errors with `let _ =` on fallible operations. Always handle errors appropriately:
    - Propagate errors with `?` when the calling function should handle them
    - Use `.log_err()` or similar when you need to ignore errors but want visibility
    - Use explicit error handling with `match` or `if let Err(...)` when you need custom logic
    - Example: avoid `let _ = client.request(...).await?;` - use `client.request(...).await?;` instead
- When implementing async operations that may fail, ensure errors propagate to the UI layer so users get meaningful feedback.
- Never create files with `mod.rs` paths - prefer `src/some_module.rs` instead of `src/some_module/mod.rs`.
- When creating new crates, prefer specifying the library root path in `Cargo.toml` using `[lib] path = "...rs"` instead of the default `lib.rs`, to maintain consistent and descriptive naming (e.g., `gpui.rs` or `main.rs`).
- Avoid creative additions unless explicitly requested
- Use full words for variable names (no abbreviations like "q" for "queue")
- Use variable shadowing to scope clones in async contexts for clarity, minimizing the lifetime of borrowed references.
  Example:
    ```rust
    executor.spawn({
        let task_ran = task_ran.clone();
        async move {
            *task_ran.borrow_mut() = true;
        }
    });
    ```

## Build & Formatting Commands

- Always run `cargo +nightly fmt` and `cargo sort -w` after editing code.
- Always run `cargo build` after completing all tasks.
- Always run `cargo doc --no-deps --document-private-items` after completing all tasks.

CI's `lint` job denies warnings on both clippy and rustdoc, so the bare commands above can pass locally yet fail CI. Reproduce the exact gate before declaring done:

- `cargo clippy --all-targets -- -D warnings` (note `--all-targets`: covers tests/benches, which plain `cargo clippy` skips).
- `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --document-private-items`. Watch for `rustdoc::invalid_html_tags`: a bare `<word>` in a doc comment (e.g. `<name>`) is parsed as an unclosed HTML tag and fails the build. Wrap such tokens in backticks, but if the doc comment is also a clap `///` help string (backticks render literally in `-h`), rephrase to drop the angle brackets instead.
- `cargo +nightly fmt --check` is what CI runs; `cargo +nightly fmt` (no `--check`) fixes it.

## Changelog

- Update `CHANGELOG.md` after every meaningful change (new features, bug fixes, breaking changes, deprecations, removals)
- Follow the [Keep a Changelog 2.0.0](https://keepachangelog.com/en/2.0.0/) format
- Add entries under the `[Unreleased]` section
- Cap each changelog entry at 100 characters
- Use only the six types (Added, Changed, Deprecated, Removed, Fixed, Security) and group entries by type
- `Fixed` means the behavior was wrong and is now correct; `Changed` means it worked as intended and now works differently
- Mark a breaking change with an inline `**Breaking:**` prefix inside its type, not in a separate section
- Lead a `Security` entry with its CVE id when one exists

## Documentation

- Update the mdBook docs under `docs/book/src/` when adding or changing user-facing features, configuration options, CLI behavior, etc.

## Prose style

- Avoid using em dashes (`—`) in writing.
