//! TLS configuration for the `serve` HTTP (and later `shuflr-wire/1`)
//! transports.
//!
//! 005 §4.2 mandates TLS 1.3 only. rustls 0.23 defaults to 1.2+1.3; we
//! explicitly disable 1.2 below. ECDHE-only handshakes are a rustls
//! default we don't override.

use std::path::Path;
use std::sync::Arc;

use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};

use crate::error::{Error, Result};

/// Build a [`ServerConfig`] for a PEM cert + key pair. If `client_ca`
/// is `Some`, require client certificates signed by any cert in that
/// PEM bundle — i.e. mTLS.
pub fn build_server_config(
    cert_pem: &Path,
    key_pem: &Path,
    client_ca: Option<&Path>,
) -> Result<Arc<ServerConfig>> {
    let certs = load_certs(cert_pem)?;
    let key = load_private_key(key_pem)?;

    // Enforce TLS 1.3 only per 005 §4.2. `with_safe_default_protocol_versions`
    // enables 1.2 + 1.3; filter to 1.3.
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| Error::Input(format!("rustls builder: {e}")))?;

    let cfg = match client_ca {
        Some(ca_path) => {
            let mut roots = RootCertStore::empty();
            for c in load_certs(ca_path)? {
                roots
                    .add(c)
                    .map_err(|e| Error::Input(format!("adding client CA to root store: {e}")))?;
            }
            let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .map_err(|e| Error::Input(format!("client verifier: {e}")))?;
            builder
                .with_client_cert_verifier(verifier)
                .with_single_cert(certs, key)
                .map_err(|e| Error::Input(format!("server cert/key: {e}")))?
        }
        None => builder
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| Error::Input(format!("server cert/key: {e}")))?,
    };
    Ok(Arc::new(cfg))
}

fn load_certs(path: &Path) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let mut r = std::io::BufReader::new(
        std::fs::File::open(path)
            .map_err(|e| Error::Input(format!("opening cert PEM {}: {e}", path.display())))?,
    );
    let certs: std::result::Result<Vec<_>, _> = rustls_pemfile::certs(&mut r).collect();
    let certs =
        certs.map_err(|e| Error::Input(format!("reading cert PEM {}: {e}", path.display())))?;
    if certs.is_empty() {
        return Err(Error::Input(format!(
            "no certificates found in {}",
            path.display()
        )));
    }
    Ok(certs)
}

fn load_private_key(path: &Path) -> Result<rustls::pki_types::PrivateKeyDer<'static>> {
    let mut r = std::io::BufReader::new(
        std::fs::File::open(path)
            .map_err(|e| Error::Input(format!("opening key PEM {}: {e}", path.display())))?,
    );
    // Accept PKCS8, RSA, or SEC1 (EC) — whichever form the caller has.
    let key = rustls_pemfile::private_key(&mut r)
        .map_err(|e| Error::Input(format!("reading key PEM {}: {e}", path.display())))?
        .ok_or_else(|| Error::Input(format!("no private key found in {}", path.display())))?;
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Build self-signed cert material in-process so tests don't depend
    // on OpenSSL / a committed fixture.
    fn test_cert_files() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        // rcgen is a test-only dep; generated material is throwaway.
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_pem = cert.cert.pem();
        let key_pem = cert.key_pair.serialize_pem();
        let cert_path = tmp.path().join("cert.pem");
        let key_path = tmp.path().join("key.pem");
        std::fs::write(&cert_path, cert_pem).unwrap();
        std::fs::write(&key_path, key_pem).unwrap();
        (tmp, cert_path, key_path)
    }

    #[test]
    fn builds_server_config_from_self_signed() {
        let (_g, cert, key) = test_cert_files();
        let cfg = build_server_config(&cert, &key, None);
        assert!(cfg.is_ok(), "{:?}", cfg.err());
    }

    #[test]
    fn rejects_missing_files() {
        let r = build_server_config(
            std::path::Path::new("/does/not/exist/cert.pem"),
            std::path::Path::new("/does/not/exist/key.pem"),
            None,
        );
        assert!(r.is_err());
    }
}
