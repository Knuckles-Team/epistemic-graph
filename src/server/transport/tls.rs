//! Native TCP TLS identity preparation. Material is loaded and validated once
//! inside the blocking worker, in certificate, private-key, then client-CA order.

#[cfg(feature = "server-tls")]
use std::io::{BufReader, Error, ErrorKind};
#[cfg(feature = "server-tls")]
use std::sync::Arc;

#[cfg(feature = "server-tls")]
use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};

/// Runtime-only TLS material for the native TCP service. Certificate contents
/// are never copied into engine configuration or logs. Supplying
/// `client_ca_path` enables mutual TLS and requires a valid client certificate.
#[derive(Clone, Debug)]
pub struct TcpTlsConfig {
    pub cert_path: String,
    pub key_path: String,
    pub client_ca_path: Option<String>,
}

/// A completely loaded and validated native-TCP identity. Filesystem access and
/// parsing stay inside [`prepare_tcp_tls`]'s blocking job; the accept loop only
/// receives the ready async acceptor and the non-secret mTLS posture.
#[derive(Clone)]
pub struct PreparedTcpTls {
    #[cfg(feature = "server-tls")]
    pub(super) acceptor: tokio_rustls::TlsAcceptor,
    pub(super) mutual_tls: bool,
}

#[cfg(feature = "server-tls")]
fn load_server_certificates(path: &str) -> std::io::Result<Vec<CertificateDer<'static>>> {
    let file = std::fs::File::open(path).map_err(|_| {
        Error::new(
            ErrorKind::InvalidInput,
            "server TLS certificate unavailable",
        )
    })?;
    let certs = CertificateDer::pem_reader_iter(BufReader::new(file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| Error::new(ErrorKind::InvalidInput, "server TLS certificate invalid"))?;
    if certs.is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "server TLS certificate invalid",
        ));
    }
    Ok(certs)
}

#[cfg(feature = "server-tls")]
fn load_server_private_key(path: &str) -> std::io::Result<PrivateKeyDer<'static>> {
    let file = std::fs::File::open(path).map_err(|_| {
        Error::new(
            ErrorKind::InvalidInput,
            "server TLS private key unavailable",
        )
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = file
            .metadata()
            .map_err(|_| {
                Error::new(
                    ErrorKind::InvalidInput,
                    "server TLS private key unavailable",
                )
            })?
            .permissions()
            .mode();
        if mode & 0o077 != 0 {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "server TLS private key permissions are too broad",
            ));
        }
    }
    PrivateKeyDer::from_pem_reader(BufReader::new(file))
        .map_err(|_| Error::new(ErrorKind::InvalidInput, "server TLS private key invalid"))
}

#[cfg(feature = "server-tls")]
fn load_client_verifier(
    path: &str,
) -> std::io::Result<Arc<dyn rustls::server::danger::ClientCertVerifier>> {
    let file = std::fs::File::open(path)
        .map_err(|_| Error::new(ErrorKind::InvalidInput, "client CA bundle unavailable"))?;
    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_reader_iter(BufReader::new(file)) {
        let cert =
            cert.map_err(|_| Error::new(ErrorKind::InvalidInput, "client CA bundle invalid"))?;
        roots
            .add(cert)
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "client CA bundle invalid"))?;
    }
    if roots.is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "client CA bundle invalid",
        ));
    }
    rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .map_err(|_| Error::new(ErrorKind::InvalidInput, "client CA bundle invalid"))
}

#[cfg(feature = "server-tls")]
fn load_server_identity(config: TcpTlsConfig) -> std::io::Result<rustls::ServerConfig> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let certs = load_server_certificates(&config.cert_path)?;
    let key = load_server_private_key(&config.key_path)?;
    let builder = rustls::ServerConfig::builder();
    let identity = if let Some(client_ca_path) = &config.client_ca_path {
        let verifier = load_client_verifier(client_ca_path)?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
    } else {
        builder.with_no_client_auth().with_single_cert(certs, key)
    };
    identity.map_err(|_| Error::new(ErrorKind::InvalidInput, "server TLS identity invalid"))
}

/// Validate configured native-TCP identity before background listeners spawn.
/// This makes missing/invalid TLS material a startup failure rather than leaving
/// an otherwise healthy UDS process with a silently absent remote listener. The
/// complete read, parse, permission check, and rustls build happen once off the
/// async executor; the resulting acceptor is reused by the live listener.
#[cfg(feature = "server-tls")]
pub async fn prepare_tcp_tls(config: TcpTlsConfig) -> std::io::Result<PreparedTcpTls> {
    let mutual_tls = config.client_ca_path.is_some();
    let server_config = ::tokio::task::spawn_blocking(move || load_server_identity(config))
        .await
        .map_err(|_| std::io::Error::other("native TCP TLS preparation worker failed"))??;

    Ok(PreparedTcpTls {
        acceptor: tokio_rustls::TlsAcceptor::from(Arc::new(server_config)),
        mutual_tls,
    })
}

#[cfg(not(feature = "server-tls"))]
pub async fn prepare_tcp_tls(_config: TcpTlsConfig) -> std::io::Result<PreparedTcpTls> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "native TCP TLS is unavailable in this build",
    ))
}
