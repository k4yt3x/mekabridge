//! Credential handling.
//!
//! Every secret mekabridge holds (the meka bearer token, bot tokens, the MCP endpoint token) is
//! wrapped in [`Secret`], whose `Debug` and `Display` impls redact. That way a config struct can be
//! `{:?}`-logged wholesale without auditing each field, which is the failure mode plain `String`
//! secrets eventually hit.

use std::{fmt, fs, path::Path};

use crate::error::{BridgeError, Result};

/// A credential plus a description of where it came from.
///
/// The origin is safe to log and is what diagnostics should print: it tells an operator which
/// environment variable or file to fix without revealing the value.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret {
    value: String,
    origin: String,
}

impl Secret {
    /// Wrap a value that was obtained from `origin`.
    pub fn new(value: impl Into<String>, origin: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            origin: origin.into(),
        }
    }

    /// The raw credential.
    ///
    /// Each call site is somewhere a secret can escape, so they are kept few and greppable.
    pub fn expose(&self) -> &str {
        &self.value
    }

    /// Where the credential was read from, safe to print.
    pub fn origin(&self) -> &str {
        &self.origin
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Secret(<redacted from {}>)", self.origin)
    }
}

impl fmt::Display for Secret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "<redacted from {}>", self.origin)
    }
}

/// Resolve the `token` / `token_file` pair that every credential-bearing config table exposes.
///
/// Exactly one of the two must be set. `label` names the table for error messages (for example
/// `[meka]` or `[[channels.telegram]] id = "main"`).
///
/// Advisories are pushed into `warnings` rather than logged, because configuration is resolved
/// before the tracing subscriber exists; the caller replays them once logging is up.
pub fn resolve(
    label: &str,
    inline: Option<&str>,
    file: Option<&Path>,
    warnings: &mut Vec<String>,
) -> Result<Secret> {
    match (inline, file) {
        (Some(_), Some(_)) => Err(BridgeError::config(format!(
            "{label} sets both `token` and `token_file`; use exactly one"
        ))),
        (None, None) => Err(BridgeError::config(format!(
            "{label} is missing a credential; set `token` or `token_file`"
        ))),
        (Some(raw), None) => {
            let value = substitute_env(label, raw)?;
            if value.is_empty() {
                return Err(BridgeError::config(format!(
                    "{label} resolved `token` to an empty string"
                )));
            }
            // A literal token in a config file is a real risk (backups, version control, world
            // readable perms), so it is allowed but never silent.
            let origin = match env_reference(raw) {
                Some(name) => format!("${{{name}}}"),
                None => {
                    warnings.push(format!(
                        "{label} holds an inline plaintext token; prefer ${{ENV_VAR}} or \
                         `token_file`"
                    ));
                    "inline config".to_string()
                }
            };
            Ok(Secret::new(value, origin))
        }
        (None, Some(path)) => {
            let raw = fs::read_to_string(path).map_err(|source| BridgeError::ConfigRead {
                path: path.to_path_buf(),
                source,
            })?;
            // Trailing newlines are what `echo 'token' > file` produces, so trimming them is the
            // difference between working and a baffling 401.
            let value = raw.trim();
            if value.is_empty() {
                return Err(BridgeError::config(format!(
                    "{label} points `token_file` at {}, which is empty",
                    path.display()
                )));
            }
            Ok(Secret::new(value, path.display().to_string()))
        }
    }
}

/// Expand every `${VAR}` occurrence in `raw` from the process environment.
pub fn substitute_env(label: &str, raw: &str) -> Result<String> {
    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        // "${" is ASCII, so `start + 2` is always a char boundary.
        let after = &rest[start + 2..];
        let end = after.find('}').ok_or_else(|| {
            BridgeError::config(format!("{label} has an unterminated `${{` in {raw:?}"))
        })?;
        let name = &after[..end];
        if name.is_empty() {
            return Err(BridgeError::config(format!(
                "{label} has an empty `${{}}` placeholder"
            )));
        }
        let value = std::env::var(name).map_err(|_| {
            BridgeError::config(format!(
                "{label} references environment variable `{name}`, which is not set"
            ))
        })?;
        out.push_str(&value);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

/// If `raw` is exactly one `${VAR}` reference and nothing else, return the variable name.
fn env_reference(raw: &str) -> Option<&str> {
    let inner = raw.strip_prefix("${")?.strip_suffix('}')?;
    if inner.is_empty() || inner.contains('$') || inner.contains('{') || inner.contains('}') {
        return None;
    }
    Some(inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_debug_and_display_redact() {
        let secret = Secret::new("sk_live_abcdef", "${TOKEN}");
        assert_eq!(format!("{secret:?}"), "Secret(<redacted from ${TOKEN}>)");
        assert_eq!(format!("{secret}"), "<redacted from ${TOKEN}>");
        assert!(!format!("{secret:?}").contains("abcdef"));
    }

    #[test]
    fn substitute_env_expands_and_keeps_surrounding_text() {
        // SAFETY: single-threaded test process section; no other thread reads the environment here.
        unsafe { std::env::set_var("MEKABRIDGE_TEST_SUBST", "middle") };
        let out = substitute_env("[test]", "left-${MEKABRIDGE_TEST_SUBST}-right").expect("expands");
        assert_eq!(out, "left-middle-right");
    }

    #[test]
    fn substitute_env_reports_missing_variable() {
        let error = substitute_env("[test]", "${MEKABRIDGE_TEST_DEFINITELY_UNSET}")
            .expect_err("must fail on unset variable");
        assert!(
            error
                .to_string()
                .contains("MEKABRIDGE_TEST_DEFINITELY_UNSET")
        );
    }

    #[test]
    fn substitute_env_rejects_unterminated_placeholder() {
        let error = substitute_env("[test]", "${OPEN").expect_err("must fail");
        assert!(error.to_string().contains("unterminated"));
    }

    #[test]
    fn substitute_env_passes_through_plain_strings() {
        assert_eq!(
            substitute_env("[test]", "no placeholders here").expect("passes through"),
            "no placeholders here"
        );
    }

    #[test]
    fn env_reference_matches_only_whole_string_references() {
        assert_eq!(env_reference("${TOKEN}"), Some("TOKEN"));
        assert_eq!(env_reference("prefix-${TOKEN}"), None);
        assert_eq!(env_reference("${}"), None);
        assert_eq!(env_reference("plain"), None);
    }

    #[test]
    fn resolve_rejects_both_sources() {
        let error = resolve(
            "[test]",
            Some("x"),
            Some(Path::new("/tmp/x")),
            &mut Vec::new(),
        )
        .expect_err("both sources must be rejected");
        assert!(error.to_string().contains("exactly one"));
    }

    #[test]
    fn resolve_rejects_neither_source() {
        let error = resolve("[test]", None, None, &mut Vec::new())
            .expect_err("missing credential must be rejected");
        assert!(error.to_string().contains("missing a credential"));
    }

    #[test]
    fn resolve_trims_token_file_contents() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("token");
        std::fs::write(&path, "sk_from_file\n").expect("write");
        let secret = resolve("[test]", None, Some(&path), &mut Vec::new()).expect("resolves");
        assert_eq!(secret.expose(), "sk_from_file");
        assert_eq!(secret.origin(), path.display().to_string());
    }

    #[test]
    fn resolve_rejects_empty_token_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("token");
        std::fs::write(&path, "   \n").expect("write");
        let error = resolve("[test]", None, Some(&path), &mut Vec::new())
            .expect_err("empty file must be rejected");
        assert!(error.to_string().contains("empty"));
    }
}
