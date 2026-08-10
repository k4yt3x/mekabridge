# Installation

## Requirements

- A working `meka serve` with a configured provider. See meka's [HTTP API docs](https://docs.meka.so/usage/http-api.html).
- A Telegram bot token from [@BotFather](https://t.me/BotFather).
- Rust 1.90 or newer if building from source.

## From source

```bash
cargo install --locked --git https://github.com/k4yt3x/mekabridge.git
```

Or build a checkout:

```bash
git clone https://github.com/k4yt3x/mekabridge.git
cd mekabridge
cargo build --release
# target/release/mekabridge
```

## Creating a config

```bash
mekabridge config init
mekabridge config path      # where it went
```

That writes a commented starter config to the platform config directory (`~/.config/mekabridge/config.toml` on Linux). Edit it, then check the setup:

```bash
mekabridge doctor
```

`doctor` reports on the config, the database, meka's reachability, the session's permission level, each channel's credentials, and the MCP port. It exits non-zero when something would actually stop the bridge working, so it is usable as a deployment gate.

## Where state lives

By default, under the platform data directory (`~/.local/share/mekabridge` on Linux):

| Path | Contents |
|------|----------|
| `mekabridge.db` | The session binding, the conversation address book, and the inbound queue |
| `attachments/` | Files downloaded from inbound messages |

Both paths are configurable under `[storage]`. The database is small; the attachment directory grows with inbound files and is swept on `[storage].attachment_retention`.

Next: [Quick Start](./quick-start.md).
