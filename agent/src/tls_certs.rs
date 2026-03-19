use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use log::info;
use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};

pub struct TlsCerts {
    pub ca_cert_path: PathBuf,
    pub ca_key_path: PathBuf,
}

/// Ensure CA certificate and key exist in data_dir. Generate if missing.
pub fn ensure_ca_certs(data_dir: &Path) -> Result<TlsCerts> {
    let cert_path = data_dir.join("ca.pem");
    let key_path = data_dir.join("ca-key.pem");

    if cert_path.exists() && key_path.exists() {
        info!("Using existing CA certs from {}", data_dir.display());
        return Ok(TlsCerts {
            ca_cert_path: cert_path,
            ca_key_path: key_path,
        });
    }

    info!("Generating new CA certificate in {}", data_dir.display());
    fs::create_dir_all(data_dir).context("Failed to create data directory")?;

    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "Red Eye Agent CA");
    params
        .distinguished_name
        .push(rcgen::DnType::OrganizationName, "Red Eye");

    let key = KeyPair::generate().context("Failed to generate CA key")?;
    let cert = params
        .self_signed(&key)
        .context("Failed to self-sign CA cert")?;

    fs::write(&cert_path, cert.pem()).context("Failed to write CA cert")?;
    fs::write(&key_path, key.serialize_pem()).context("Failed to write CA key")?;

    info!("CA certificate generated");

    Ok(TlsCerts {
        ca_cert_path: cert_path,
        ca_key_path: key_path,
    })
}
