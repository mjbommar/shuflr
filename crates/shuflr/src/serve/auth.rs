//! Auth policy for the `serve` HTTP transport.
//!
//! PR-31 options per 005 §4.3:
//!
//!   --auth=none    (default)
//!   --auth=bearer  --auth-tokens <file>
//!   --auth=mtls    --tls-cert / --tls-key / --tls-client-ca <file>
//!
//! mTLS is enforced by rustls at the TLS layer — the HTTP handler
//! sees only connections that already presented an acceptable client
//! cert, so there's nothing application-level to check. Bearer
//! validation runs per-request on the `Authorization: Bearer <token>`
//! header. Tokens are compared with [`subtle::ConstantTimeEq`] to
//! blunt timing side channels.

use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use subtle::ConstantTimeEq;

use crate::error::{Error, Result};

/// How `serve` authenticates inbound requests.
#[derive(Clone)]
pub enum Auth {
    /// No auth. Suitable for loopback / UDS / trusted sidecar.
    None,
    /// Bearer-token: each request must present a matching token on
    /// the `Authorization: Bearer ...` header. Tokens are reloaded
    /// from the source file on SIGHUP (unix only).
    Bearer(BearerConfig),
    /// mTLS: auth enforced at the TLS layer by rustls; nothing to do
    /// per-request.
    Mtls,
}

#[derive(Clone)]
pub struct BearerConfig {
    pub tokens_file: PathBuf,
    pub tokens: Arc<RwLock<Vec<Vec<u8>>>>,
}

impl std::fmt::Debug for Auth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Auth::None => f.write_str("Auth::None"),
            Auth::Bearer(cfg) => {
                let n = cfg.tokens.read().map(|t| t.len()).unwrap_or(0);
                write!(
                    f,
                    "Auth::Bearer(file={}, {n} tokens)",
                    cfg.tokens_file.display()
                )
            }
            Auth::Mtls => f.write_str("Auth::Mtls"),
        }
    }
}

impl Auth {
    /// Load bearer tokens from a newline-delimited file. Blank lines
    /// and lines starting with `#` are ignored. Trailing whitespace
    /// per line is stripped.
    pub fn bearer_from_file(path: PathBuf) -> Result<Self> {
        let tokens = load_tokens(&path)?;
        if tokens.is_empty() {
            return Err(Error::Input(format!(
                "--auth-tokens {} contains no tokens",
                path.display()
            )));
        }
        Ok(Auth::Bearer(BearerConfig {
            tokens_file: path,
            tokens: Arc::new(RwLock::new(tokens)),
        }))
    }

    /// Re-read the token file in place. Called by the SIGHUP handler.
    /// Errors are logged but don't propagate — a bad file on reload
    /// leaves the in-memory set intact.
    pub fn reload_if_bearer(&self) {
        if let Auth::Bearer(cfg) = self {
            match load_tokens(&cfg.tokens_file) {
                Ok(new_tokens) if !new_tokens.is_empty() => {
                    if let Ok(mut guard) = cfg.tokens.write() {
                        let old = guard.len();
                        *guard = new_tokens;
                        let new = guard.len();
                        tracing::info!(file = %cfg.tokens_file.display(), old, new, "bearer tokens reloaded");
                    }
                }
                Ok(_) => {
                    tracing::warn!(
                        file = %cfg.tokens_file.display(),
                        "reload: empty token file; keeping previous tokens"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        file = %cfg.tokens_file.display(),
                        err = %e,
                        "reload: failed to read token file; keeping previous tokens",
                    );
                }
            }
        }
    }

    /// Returns `true` if the given `Authorization` header value (or
    /// None if missing) satisfies the policy.
    pub fn verify_http_header(&self, auth_header: Option<&str>) -> AuthOutcome {
        match self {
            Auth::None | Auth::Mtls => AuthOutcome::Ok,
            Auth::Bearer(cfg) => {
                let Some(header_val) = auth_header else {
                    return AuthOutcome::Missing;
                };
                let Some(raw) = header_val.strip_prefix("Bearer ") else {
                    return AuthOutcome::Malformed;
                };
                let raw = raw.trim();
                if raw.is_empty() {
                    return AuthOutcome::Malformed;
                }
                let Ok(guard) = cfg.tokens.read() else {
                    return AuthOutcome::Internal;
                };
                for tok in guard.iter() {
                    if tok.as_slice().ct_eq(raw.as_bytes()).unwrap_u8() == 1 {
                        return AuthOutcome::Ok;
                    }
                }
                AuthOutcome::Mismatch
            }
        }
    }
}

/// Result of an auth check. The HTTP layer maps these to status codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthOutcome {
    Ok,
    /// No `Authorization` header on a bearer-protected endpoint.
    Missing,
    /// Header present but not `Bearer <token>` or empty token.
    Malformed,
    /// Token not in the allow-list.
    Mismatch,
    /// RwLock poisoned / file IO while reloading.
    Internal,
}

fn load_tokens(path: &std::path::Path) -> Result<Vec<Vec<u8>>> {
    use std::io::BufRead as _;
    let f = std::fs::File::open(path)
        .map_err(|e| Error::Input(format!("opening {}: {e}", path.display())))?;
    let r = std::io::BufReader::new(f);
    let mut out = Vec::new();
    for (lineno, line) in r.lines().enumerate() {
        let line = line
            .map_err(|e| Error::Input(format!("reading {} line {lineno}: {e}", path.display())))?;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        out.push(trimmed.as_bytes().to_vec());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_tokens_file(body: &str) -> (tempfile::NamedTempFile, PathBuf) {
        let tf = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tf.path(), body).unwrap();
        let p = tf.path().to_path_buf();
        (tf, p)
    }

    #[test]
    fn none_accepts_anything() {
        let a = Auth::None;
        assert_eq!(a.verify_http_header(None), AuthOutcome::Ok);
        assert_eq!(a.verify_http_header(Some("Bearer xyz")), AuthOutcome::Ok);
    }

    #[test]
    fn bearer_accepts_known_token() {
        let (_g, p) = write_tokens_file("abc123\nother-token\n");
        let a = Auth::bearer_from_file(p).unwrap();
        assert_eq!(a.verify_http_header(Some("Bearer abc123")), AuthOutcome::Ok);
        assert_eq!(
            a.verify_http_header(Some("Bearer other-token")),
            AuthOutcome::Ok
        );
    }

    #[test]
    fn bearer_rejects_wrong_token() {
        let (_g, p) = write_tokens_file("abc123\n");
        let a = Auth::bearer_from_file(p).unwrap();
        assert_eq!(
            a.verify_http_header(Some("Bearer zzz")),
            AuthOutcome::Mismatch
        );
    }

    #[test]
    fn bearer_rejects_missing_header() {
        let (_g, p) = write_tokens_file("abc123\n");
        let a = Auth::bearer_from_file(p).unwrap();
        assert_eq!(a.verify_http_header(None), AuthOutcome::Missing);
    }

    #[test]
    fn bearer_rejects_malformed_scheme() {
        let (_g, p) = write_tokens_file("abc123\n");
        let a = Auth::bearer_from_file(p).unwrap();
        assert_eq!(
            a.verify_http_header(Some("Basic abc123")),
            AuthOutcome::Malformed
        );
        assert_eq!(
            a.verify_http_header(Some("Bearer ")),
            AuthOutcome::Malformed
        );
    }

    #[test]
    fn bearer_rejects_empty_file() {
        let (_g, p) = write_tokens_file("# comments\n\n");
        assert!(Auth::bearer_from_file(p).is_err());
    }

    #[test]
    fn bearer_ignores_comments_and_blanks() {
        let (_g, p) = write_tokens_file("\n# comment\nalpha\n\nbeta\n");
        let a = Auth::bearer_from_file(p).unwrap();
        assert_eq!(a.verify_http_header(Some("Bearer alpha")), AuthOutcome::Ok);
        assert_eq!(a.verify_http_header(Some("Bearer beta")), AuthOutcome::Ok);
    }

    #[test]
    fn reload_picks_up_new_tokens() {
        let tf = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tf.path(), "old\n").unwrap();
        let a = Auth::bearer_from_file(tf.path().to_path_buf()).unwrap();
        assert_eq!(
            a.verify_http_header(Some("Bearer new")),
            AuthOutcome::Mismatch
        );

        std::fs::write(tf.path(), "old\nnew\n").unwrap();
        a.reload_if_bearer();
        assert_eq!(a.verify_http_header(Some("Bearer new")), AuthOutcome::Ok);
        assert_eq!(a.verify_http_header(Some("Bearer old")), AuthOutcome::Ok);
    }
}
