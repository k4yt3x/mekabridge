//! Hosting for the MCP server: streamable HTTP (the normal deployment) and stdio (for MCP Inspector
//! and child-process launches).
//!
//! The HTTP listener is bound before the bridge needs meka for anything, so a port conflict fails
//! startup outright. meka retries a failed MCP connect in the background with backoff, so a meka
//! that boots first recovers on its own; until it does, `[mcp].strict` refuses turns, which is why
//! coming up promptly still matters.

use std::sync::Arc;

use axum::{
    Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::get,
};
use rmcp::{
    ServiceExt,
    transport::{
        stdio,
        streamable_http_server::{
            StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
        },
    },
};
use tokio_util::sync::CancellationToken;

use crate::{
    config::{McpConfig, McpTransport, secret::Secret},
    error::{BridgeError, Result},
    mcp::{BridgeMcpServer, OutboundSink},
};

/// A bound but not yet serving MCP endpoint.
///
/// Binding is separate from serving so startup fails immediately on a port conflict, before the
/// channels connect and the queue starts moving, and so callers can learn the address an ephemeral
/// port resolved to.
pub enum McpServer {
    Http {
        listener: tokio::net::TcpListener,
        local_addr: std::net::SocketAddr,
    },
    Stdio,
}

impl McpServer {
    /// The bound address, or `None` on stdio.
    pub const fn local_addr(&self) -> Option<std::net::SocketAddr> {
        match self {
            Self::Http { local_addr, .. } => Some(*local_addr),
            Self::Stdio => None,
        }
    }
}

/// Bind the endpoint described by `config` without serving on it yet.
pub async fn bind(config: &McpConfig) -> Result<McpServer> {
    match config.transport {
        McpTransport::Stdio => Ok(McpServer::Stdio),
        McpTransport::Http => {
            let listener = tokio::net::TcpListener::bind(config.bind)
                .await
                .map_err(|source| {
                    BridgeError::config(format!(
                        "failed to bind the MCP server to {}: {source}",
                        config.bind
                    ))
                })?;
            let local_addr = listener.local_addr().unwrap_or(config.bind);
            Ok(McpServer::Http {
                listener,
                local_addr,
            })
        }
    }
}

/// Serve MCP until `shutdown` fires.
pub async fn run(
    server: McpServer,
    config: &McpConfig,
    sink: Arc<dyn OutboundSink>,
    shutdown: CancellationToken,
) -> Result<()> {
    match server {
        McpServer::Http {
            listener,
            local_addr,
        } => serve_http(listener, local_addr, config, sink, shutdown).await,
        McpServer::Stdio => serve_stdio(sink, shutdown).await,
    }
}

/// Bind and serve in one step.
pub async fn serve(
    config: &McpConfig,
    sink: Arc<dyn OutboundSink>,
    shutdown: CancellationToken,
) -> Result<()> {
    let server = bind(config).await?;
    run(server, config, sink, shutdown).await
}

async fn serve_stdio(sink: Arc<dyn OutboundSink>, shutdown: CancellationToken) -> Result<()> {
    let server = BridgeMcpServer::new(sink);
    let running = server.serve(stdio()).await.map_err(|error| {
        BridgeError::config(format!("failed to start the stdio MCP server: {error}"))
    })?;
    tracing::info!("MCP server listening on stdio");
    tokio::select! {
        result = running.waiting() => {
            if let Err(error) = result {
                tracing::warn!("stdio MCP server stopped: {}", error);
            }
        }
        () = shutdown.cancelled() => {
            tracing::info!("MCP server shutting down");
        }
    }
    Ok(())
}

async fn serve_http(
    listener: tokio::net::TcpListener,
    local: std::net::SocketAddr,
    config: &McpConfig,
    sink: Arc<dyn OutboundSink>,
    shutdown: CancellationToken,
) -> Result<()> {
    let service = StreamableHttpService::new(
        move || Ok(BridgeMcpServer::new(Arc::clone(&sink))),
        Arc::new(LocalSessionManager::default()),
        streamable_config(config, &shutdown),
    );

    let mut router = Router::new().nest_service(&config.path, service);
    if config.health {
        router = router
            .route("/health/live", get(live))
            .route("/health/ready", get(ready));
    }
    if let Some(token) = &config.token {
        router = router.layer(axum::middleware::from_fn_with_state(
            Arc::new(token.clone()),
            require_bearer,
        ));
    }

    tracing::info!(
        "MCP server listening on http://{}{} ({})",
        local,
        config.path,
        if config.token.is_some() {
            "bearer token required"
        } else {
            "unauthenticated"
        }
    );
    if config.token.is_none() && !config.bind.ip().is_loopback() {
        tracing::warn!(
            "the MCP endpoint is bound to a non-loopback address with no `[mcp].token`; anyone \
             who can reach {} can send messages as the agent",
            local
        );
    }

    axum::serve(listener, router)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
        .map_err(BridgeError::Io)
}

fn streamable_config(
    config: &McpConfig,
    shutdown: &CancellationToken,
) -> StreamableHttpServerConfig {
    // `StreamableHttpServerConfig` is `#[non_exhaustive]`, so it has to be built by mutating the
    // default rather than with a struct literal.
    let mut streamable = StreamableHttpServerConfig::default();
    streamable.cancellation_token = shutdown.child_token();
    // rmcp defaults to accepting loopback `Host` values only, as a DNS-rebinding guard. A bind on a
    // routable address needs the operator to name the hosts meka will use.
    if !config.allowed_hosts.is_empty() {
        streamable.allowed_hosts.clone_from(&config.allowed_hosts);
    }
    streamable
}

async fn live() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn ready() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

/// Reject requests that do not carry the configured bearer token.
///
/// Health probes stay open so a container orchestrator does not need the credential.
async fn require_bearer(
    State(expected): State<Arc<Secret>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let path = request.uri().path();
    if path == "/health/live" || path == "/health/ready" {
        return next.run(request).await;
    }
    if bearer_matches(request.headers(), expected.expose()) {
        next.run(request).await
    } else {
        tracing::warn!("rejected an MCP request with a missing or invalid bearer token");
        (StatusCode::UNAUTHORIZED, "unauthorized").into_response()
    }
}

fn bearer_matches(headers: &HeaderMap, expected: &str) -> bool {
    let Some(value) = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    let Some(presented) = value.strip_prefix("Bearer ").map(str::trim) else {
        return false;
    };
    constant_time_eq(presented.as_bytes(), expected.as_bytes())
}

/// Compare without an early return on the first differing byte.
///
/// The length check leaks the token's length, which is not sensitive; what matters is that a
/// matching prefix takes the same time as a non-matching one, so the comparison cannot be used to
/// recover the token byte by byte.
fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut difference = 0_u8;
    for (a, b) in left.iter().zip(right.iter()) {
        difference |= a ^ b;
    }
    difference == 0
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;

    fn headers_with(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if let Ok(parsed) = HeaderValue::from_str(value) {
            headers.insert(axum::http::header::AUTHORIZATION, parsed);
        }
        headers
    }

    #[test]
    fn bearer_matches_the_configured_token() {
        assert!(bearer_matches(&headers_with("Bearer secret"), "secret"));
    }

    #[test]
    fn bearer_rejects_wrong_or_malformed_credentials() {
        assert!(!bearer_matches(&headers_with("Bearer wrong"), "secret"));
        assert!(!bearer_matches(&headers_with("secret"), "secret"));
        assert!(!bearer_matches(&headers_with("Basic secret"), "secret"));
        assert!(!bearer_matches(&HeaderMap::new(), "secret"));
    }

    #[test]
    fn bearer_rejects_a_prefix_of_the_token() {
        assert!(!bearer_matches(&headers_with("Bearer sec"), "secret"));
    }

    #[test]
    fn constant_time_eq_matches_equality() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(constant_time_eq(b"", b""));
    }
}
