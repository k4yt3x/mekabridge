# Installation

## Requirements

- Meka 0.80 or later, running as `meka serve` with a configured provider. See Meka's [HTTP API docs](https://docs.meka.bot/usage/http-api.html).
- A bot token for each platform you connect: a Telegram one from [@BotFather](https://t.me/BotFather), a Discord one from the [Developer Portal](https://discord.com/developers/applications).
- Rust 1.89 or newer if building from source.

## Pre-built binaries

Download the latest release for your platform from the [GitHub Releases](https://github.com/k4yt3x/meka-bridge/releases/latest) page.

| Platform | Archive |
|----------|---------|
| Linux (x86_64) | `meka-bridge-linux-amd64.tar.gz` |
| macOS (Apple Silicon) | `meka-bridge-macos-arm64.tar.gz` |
| Windows (x86_64) | `meka-bridge-windows-amd64.zip` |

Extract the binary and place it somewhere on your `$PATH`. The [systemd unit](../usage/operations.md#systemd) runs it from `/usr/local/bin`:

```bash
# Linux/macOS
tar -xzf meka-bridge-*.tar.gz
sudo cp meka-bridge /usr/local/bin/
```

## From source

```bash
cargo install --locked --git https://github.com/k4yt3x/meka-bridge.git
```

Or build a checkout:

```bash
git clone https://github.com/k4yt3x/meka-bridge.git
cd meka-bridge
cargo build --release
# target/release/meka-bridge
```

## Creating a config

```bash
meka-bridge config init
meka-bridge config path      # where it went
```

That writes a commented starter config to the platform config directory (`~/.config/meka-bridge/config.toml` on Linux). Edit it, then check the setup:

```bash
meka-bridge doctor
```

`doctor` reports on the config, the database, Meka's reachability, the session's permission level, each channel's credentials, and the MCP port. It exits non-zero when something would actually stop the bridge working, so it is usable as a deployment gate.

## Where state lives

By default, under the platform data directory (`~/.local/share/meka-bridge` on Linux):

| Path | Contents |
|------|----------|
| `meka-bridge.db` | The session binding, the conversation address book, and the inbound queue |
| `attachments/` | Files downloaded from inbound messages |

Both paths are configurable under `[storage]`. The database is small; the attachment directory grows with inbound files and is swept on `[storage].attachment_retention`.

Next: [Quick Start](./quick-start.md).
