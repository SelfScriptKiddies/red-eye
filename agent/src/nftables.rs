use std::process::Command;

use anyhow::{bail, Result};
use log::{debug, info, warn};
use serde::Deserialize;

const TABLE_NAME: &str = "red-eye-agent";
const CHAIN_NAME: &str = "prerouting";

/// Accepts both a bare address and a CIDR-styled one ("10.0.0.1/32") —
/// stream IPs are stored CIDR-style and older frontends sent them verbatim;
/// the /prefix is simply ignored (a ban is always /32-equivalent).
fn validate_ip(ip: &str) -> Result<()> {
    use std::net::IpAddr;
    let bare = ip.trim().split('/').next().unwrap_or(ip.trim());
    bare.parse::<IpAddr>()
        .map_err(|_| anyhow::anyhow!("Invalid IP address: {}", ip))?;
    Ok(())
}

/// True when the `nft` binary exists and runs (probed once, cached). The
/// heartbeat reports it so the UI can explain why proxy/ban buttons fail
/// on hosts without nftables instead of failing silently.
pub fn nft_binary_available() -> bool {
    use std::sync::atomic::{AtomicU8, Ordering};
    static CACHE: AtomicU8 = AtomicU8::new(0);
    match CACHE.load(Ordering::Relaxed) {
        1 => true,
        2 => false,
        _ => {
            let ok = Command::new("nft")
                .arg("--version")
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false);
            CACHE.store(if ok { 1 } else { 2 }, Ordering::Relaxed);
            ok
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RedirectRule {
    pub protocol: String,
    pub original_port: u16,
    pub redirect_port: u16,
}

pub struct NftablesManager {
    initialized: bool,
    rules: Vec<RedirectRule>,
    banned_ips: Vec<String>,
}

impl NftablesManager {
    pub fn new() -> Self {
        Self {
            initialized: false,
            rules: Vec::new(),
            banned_ips: Vec::new(),
        }
    }

    /// Replace all redirect rules with a new set.
    pub fn apply_rules(&mut self, rules: Vec<RedirectRule>) -> Result<()> {
        self.ensure_table()?;
        self.flush_chain()?;

        for rule in &rules {
            self.add_redirect_rule(rule)?;
        }

        info!("Applied {} nftables redirect rules", rules.len());
        self.rules = rules;
        Ok(())
    }

    /// Add a single redirect rule (for auto-management by harpoon proxy).
    /// Skips if a redirect for this original_port already exists.
    pub fn add_redirect(&mut self, rule: RedirectRule) -> Result<()> {
        // Deduplicate: if a rule for this port exists, update it
        if let Some(existing) = self.rules.iter().position(|r| r.original_port == rule.original_port) {
            if self.rules[existing].redirect_port == rule.redirect_port
                && self.rules[existing].protocol == rule.protocol
            {
                debug!("nftables redirect {} {} -> {} already exists, skipping",
                    rule.protocol, rule.original_port, rule.redirect_port);
                return Ok(());
            }
            // Different target — remove old, add new
            self.rules.remove(existing);
            self.flush_chain()?;
            for r in &self.rules.clone() {
                self.add_redirect_rule(r)?;
            }
            // Re-add ban rules after flush
            for ip in &self.banned_ips.clone() {
                let _ = run_nft(&format!(
                    "add rule inet {} {} ip saddr {} drop",
                    TABLE_NAME, CHAIN_NAME, ip
                ));
            }
        }

        self.ensure_table()?;
        self.add_redirect_rule(&rule)?;
        info!(
            "Added nftables redirect: {} {} -> {}",
            rule.protocol, rule.original_port, rule.redirect_port
        );
        self.rules.push(rule);
        Ok(())
    }

    /// Remove redirect rules for a specific original port.
    pub fn remove_redirect(&mut self, original_port: u16) -> Result<()> {
        let had_rules = self.rules.len();
        self.rules.retain(|r| r.original_port != original_port);

        if self.rules.len() < had_rules {
            // Re-apply all remaining rules (flush + re-add)
            self.flush_chain()?;
            for rule in &self.rules.clone() {
                self.add_redirect_rule(rule)?;
            }
            info!(
                "Removed nftables redirect for port {}",
                original_port
            );
        }
        Ok(())
    }

    /// Ban an IP address (drop all traffic from it).
    pub fn ban_ip(&mut self, ip: &str) -> Result<()> {
        validate_ip(ip)?;
        self.ensure_table()?;
        // Add drop rule for this IP
        run_nft(&format!(
            "add rule inet {} {} ip saddr {} drop",
            TABLE_NAME, CHAIN_NAME, ip
        ))?;
        if !self.banned_ips.contains(&ip.to_string()) {
            self.banned_ips.push(ip.to_string());
        }
        info!("Banned IP: {}", ip);
        Ok(())
    }

    /// Unban an IP address.
    pub fn unban_ip(&mut self, ip: &str) -> Result<()> {
        validate_ip(ip)?;
        self.banned_ips.retain(|i| i != ip);
        // Re-apply all rules (flush chain, re-add redirects + remaining bans)
        if self.initialized {
            self.flush_chain()?;
            for rule in &self.rules.clone() {
                self.add_redirect_rule(rule)?;
            }
            for banned in &self.banned_ips.clone() {
                let _ = run_nft(&format!(
                    "add rule inet {} {} ip saddr {} drop",
                    TABLE_NAME, CHAIN_NAME, banned
                ));
            }
        }
        info!("Unbanned IP: {}", ip);
        Ok(())
    }

    /// List currently banned IPs.
    pub fn banned_ips(&self) -> &[String] {
        &self.banned_ips
    }

    /// Delete the entire nftables table.
    pub fn clear(&mut self) -> Result<()> {
        if self.initialized {
            match run_nft(&format!("delete table inet {}", TABLE_NAME)) {
                Ok(_) => info!("Cleared all nftables rules"),
                Err(e) => warn!("Failed to clear nftables table: {}", e),
            }
            self.initialized = false;
        }
        self.rules.clear();
        Ok(())
    }

    /// Current active rules (for status reporting).
    pub fn active_rules(&self) -> &[RedirectRule] {
        &self.rules
    }

    fn ensure_table(&mut self) -> Result<()> {
        if self.initialized {
            return Ok(());
        }

        run_nft(&format!("add table inet {}", TABLE_NAME))?;
        run_nft(&format!(
            "add chain inet {} {} {{ type nat hook prerouting priority dstnat \\; }}",
            TABLE_NAME, CHAIN_NAME
        ))?;

        self.initialized = true;
        debug!("nftables table '{}' initialized", TABLE_NAME);
        Ok(())
    }

    fn flush_chain(&self) -> Result<()> {
        run_nft(&format!(
            "flush chain inet {} {}",
            TABLE_NAME, CHAIN_NAME
        ))
    }

    fn add_redirect_rule(&self, rule: &RedirectRule) -> Result<()> {
        if rule.protocol != "tcp" && rule.protocol != "udp" {
            bail!("Invalid protocol: {}", rule.protocol);
        }
        run_nft(&format!(
            "add rule inet {} {} {} dport {} redirect to :{}",
            TABLE_NAME, CHAIN_NAME, rule.protocol, rule.original_port, rule.redirect_port
        ))
    }
}

impl Drop for NftablesManager {
    fn drop(&mut self) {
        if self.initialized {
            let _ = self.clear();
        }
    }
}

fn run_nft(cmd: &str) -> Result<()> {
    let full_cmd = format!("nft {}", cmd);
    debug!("Running: {}", full_cmd);

    let output = Command::new("sh")
        .arg("-c")
        .arg(&full_cmd)
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("nft command failed: {}", stderr.trim());
    }

    Ok(())
}
