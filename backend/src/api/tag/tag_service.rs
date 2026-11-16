use crate::api::tag::tag_dto::{
    ACTIONS, DIRECTIONS, GROUP_BYS, IP_ENDPOINTS, MATCHER_TYPES, MAX_PATTERN_LEN, Tag, TagName,
    TagNameCreate, TagValidationError,
};
use crate::api::tag::tag_repository::TagRepository;
use ipnetwork::IpNetwork;
use sqlx::PgPool;
use std::net::IpAddr;

#[derive(Debug)]
pub enum TagServiceError {
    Sqlx(sqlx::Error),
    Validation(TagValidationError),
}

impl From<sqlx::Error> for TagServiceError {
    fn from(e: sqlx::Error) -> Self {
        TagServiceError::Sqlx(e)
    }
}

impl From<TagValidationError> for TagServiceError {
    fn from(e: TagValidationError) -> Self {
        TagServiceError::Validation(e)
    }
}

pub(crate) struct TagService {
    repository: TagRepository,
}

/// Validate a filter definition completely before persistence: matcher
/// variants, action combinations and threshold fields. Expression source
/// compilation is layered on by the caller that owns the DSL.
fn validate(tag_name: &TagNameCreate, action: &str) -> Result<(), TagValidationError> {
    let fail = |msg: &str| Err(TagValidationError(msg.to_string()));

    if tag_name.name.trim().is_empty() || tag_name.name.len() > 255 {
        return fail("name must be 1-255 characters");
    }
    if !MATCHER_TYPES.contains(&tag_name.template_type.as_str()) {
        return fail(
            "template_type must be one of regex | substr | bsubstr | ip_cidr | expression",
        );
    }
    if !DIRECTIONS.contains(&tag_name.direction.as_str()) {
        return fail("direction must be one of in | out | both");
    }
    if !ACTIONS.contains(&action) {
        return fail("action must be one of tag | flag | alert | block | block_connection");
    }

    match tag_name.template_type.as_str() {
        "ip_cidr" => {
            if !tag_name.template.is_empty() {
                return fail("ip_cidr filters must not carry a payload pattern");
            }
            // Normalized here as well as in the caller: validation must not
            // depend on call order.
            normalize_cidr(tag_name.ip_network.as_deref().unwrap_or(""))?;
            match tag_name.ip_endpoint.as_deref() {
                Some(ep) if IP_ENDPOINTS.contains(&ep) => {}
                _ => return fail("ip_cidr filters require ip_endpoint = src | dst"),
            }
        }
        "expression" => {
            if tag_name.ip_network.is_some() || tag_name.ip_endpoint.is_some() {
                return fail("expression filters must not carry ip_cidr fields");
            }
            match tag_name.expression_source.as_deref() {
                Some(src) if !src.trim().is_empty() => {
                    if src.len() > crate::api::tag::tag_dto::MAX_EXPRESSION_SOURCE_LEN {
                        return fail("expression source exceeds the length limit");
                    }
                    // The shared DSL must accept the source with a boolean
                    // result type; a compile failure is rejected before
                    // persistence, never shipped to consumers.
                    if let Err(e) = filter_dsl::compile(src) {
                        return fail(&format!("expression does not compile: {}", e));
                    }
                }
                _ => return fail("expression filters require expression_source"),
            }
            if !tag_name.template.is_empty() {
                return fail("expression filters must not carry a payload pattern");
            }
        }
        _ => {
            if tag_name.ip_network.is_some() || tag_name.ip_endpoint.is_some() {
                return fail("only ip_cidr filters carry ip_network/ip_endpoint");
            }
            if tag_name.expression_source.is_some() {
                return fail("only expression filters carry expression_source");
            }
            if tag_name.template.is_empty() {
                return fail("payload filters require a template pattern");
            }
            if tag_name.template.len() > MAX_PATTERN_LEN {
                return fail("template pattern exceeds 255 characters");
            }
            if tag_name.template_type == "bsubstr" {
                // Invalid hexadecimal is rejected rather than silently
                // changing meaning (raw-byte fallback is a legacy-only
                // read path).
                if hex::decode(&tag_name.template).is_err() {
                    return fail("bsubstr template must be valid hexadecimal");
                }
            }
            if tag_name.template_type == "regex" {
                validate_regex(&tag_name.template)?;
            }
        }
    }

    // Thresholds gate only alert and block actions; tag and flag reject
    // them. Immediate alert/block filters omit the threshold entirely.
    let threshold_present = tag_name.threshold_count.is_some()
        || tag_name.threshold_window_secs.is_some()
        || tag_name.threshold_group_by.is_some()
        || tag_name.threshold_cooldown_secs.is_some()
        || tag_name.threshold_block_secs.is_some();
    let threshold_action = matches!(action, "alert" | "block");
    if !threshold_action && threshold_present {
        return fail("thresholds are valid only for alert and block actions");
    }
    if threshold_action && threshold_present {
        let count = tag_name.threshold_count.unwrap_or(0);
        let window = tag_name.threshold_window_secs.unwrap_or(0);
        if count <= 0 {
            return fail("threshold_count must be a positive integer");
        }
        if window <= 0 {
            return fail("threshold_window_secs must be a positive integer");
        }
        match tag_name.threshold_group_by.as_deref() {
            Some(g) if GROUP_BYS.contains(&g) => {}
            _ => return fail("threshold_group_by must be one of remote_ip | stream | service"),
        }
        if tag_name.threshold_cooldown_secs.unwrap_or(0) < 0 {
            return fail("threshold_cooldown_secs must be non-negative");
        }
        if action == "block" && tag_name.threshold_block_secs.unwrap_or(0) <= 0 {
            return fail("block thresholds require a positive threshold_block_secs");
        }
    } else if threshold_action {
        // A partially filled threshold is an operator mistake, not an
        // immediate rule with stray fields.
        let filled = tag_name.threshold_count.is_some()
            || tag_name.threshold_window_secs.is_some()
            || tag_name.threshold_group_by.is_some()
            || tag_name.threshold_block_secs.is_some();
        if filled {
            return fail("threshold configuration is incomplete");
        }
    }

    if !tag_name.service_targets_global && tag_name.service_ids.is_empty() {
        // Explicit service targeting with an empty set is kept legal at
        // write time; the service-deletion path disables such filters and
        // the UI flags them as requiring targets.
    }

    Ok(())
}

/// Normalize a single address to its host prefix and a network to its
/// masked form, so host bits cannot create visually different rules with
/// identical behavior.
pub(crate) fn normalize_cidr(input: &str) -> Result<IpNetwork, TagValidationError> {
    let input = input.trim();
    let fail = || TagValidationError(format!("invalid IP address or CIDR network: {input}"));

    if let Ok(addr) = input.parse::<IpAddr>() {
        let prefix = if addr.is_ipv4() { 32 } else { 128 };
        return IpNetwork::new(addr, prefix).map_err(|_| fail());
    }

    let net: IpNetwork = input.parse().map_err(|_| fail())?;
    let masked = mask_host_bits(net.ip(), net.prefix());
    IpNetwork::new(masked, net.prefix()).map_err(|_| fail())
}

/// Validate the regex source compiles within the documented compiled-size
/// limit; rejected at save time, never shipped to consumers.
fn validate_regex(pattern: &str) -> Result<(), TagValidationError> {
    regex::RegexBuilder::new(pattern)
        .size_limit(crate::api::tag::tag_dto::MAX_REGEX_COMPILED_SIZE)
        .build()
        .map(|_| ())
        .map_err(|e| TagValidationError(format!("regex does not compile: {}", e)))
}

fn mask_host_bits(addr: IpAddr, prefix: u8) -> IpAddr {
    match addr {
        IpAddr::V4(v4) => {
            let bits = u32::from(v4);
            let mask = if prefix == 0 {
                0
            } else {
                u32::MAX << (32 - prefix as u32)
            };
            IpAddr::V4(std::net::Ipv4Addr::from(bits & mask))
        }
        IpAddr::V6(v6) => {
            let bits = u128::from(v6);
            let mask = if prefix == 0 {
                0
            } else {
                u128::MAX << (128 - prefix as u32)
            };
            IpAddr::V6(std::net::Ipv6Addr::from(bits & mask))
        }
    }
}

impl TagService {
    pub fn new(repository: TagRepository) -> Self {
        Self { repository }
    }

    pub fn new_from_pgpool(pool: PgPool) -> Self {
        Self {
            repository: TagRepository::new(pool),
        }
    }

    pub async fn create_tag_name(
        &self,
        tag_name: TagNameCreate,
    ) -> Result<TagName, TagServiceError> {
        let action = tag_name.resolved_action();
        validate(&tag_name, &action)?;
        let ip_network = if tag_name.template_type == "ip_cidr" {
            Some(normalize_cidr(
                tag_name.ip_network.as_deref().unwrap_or(""),
            )?)
        } else {
            None
        };
        Ok(self
            .repository
            .create_tag_name(tag_name, action, ip_network)
            .await?)
    }

    pub async fn get_tags_by_packet_id(&self, packet_id: i64) -> Result<Vec<Tag>, sqlx::Error> {
        self.repository.get_tags_by_packet_id(packet_id).await
    }

    pub async fn get_tags_by_stream_id(&self, stream_id: i64) -> Result<Vec<Tag>, sqlx::Error> {
        self.repository.get_tags_by_stream_id(stream_id).await
    }

    pub async fn get_tag_name_by_id(&self, id: i64) -> Result<TagName, sqlx::Error> {
        self.repository.get_tag_name_by_id(id).await
    }

    pub async fn update_tag_name(
        &self,
        id: i64,
        tag_name: TagNameCreate,
    ) -> Result<TagName, TagServiceError> {
        let action = tag_name.resolved_action();
        validate(&tag_name, &action)?;
        let ip_network = if tag_name.template_type == "ip_cidr" {
            Some(normalize_cidr(
                tag_name.ip_network.as_deref().unwrap_or(""),
            )?)
        } else {
            None
        };
        Ok(self
            .repository
            .update_tag_name(id, tag_name, action, ip_network)
            .await?)
    }

    pub async fn set_tag_name_enabled(&self, id: i64, enabled: bool) -> Result<i64, sqlx::Error> {
        self.repository.set_tag_name_enabled(id, enabled).await
    }

    pub async fn delete_tag(&self, id: i64) -> Result<(), sqlx::Error> {
        self.repository.delete_tag(id).await
    }

    pub async fn delete_tag_name(&self, id: i64) -> Result<i64, sqlx::Error> {
        self.repository.delete_tag_name(id).await
    }

    pub async fn get_all_tag_names(&self) -> Result<Vec<TagName>, sqlx::Error> {
        self.repository.get_all_tag_names().await
    }

    pub async fn get_tag_metrics(
        &self,
        minutes: i32,
    ) -> Result<Vec<(i64, String, i64, i64)>, sqlx::Error> {
        self.repository.get_tag_metrics(minutes).await
    }

    pub async fn get_flag_balance(
        &self,
        minutes: i32,
    ) -> Result<Vec<(i64, String, i64, i64)>, sqlx::Error> {
        self.repository.get_flag_balance(minutes).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> TagNameCreate {
        TagNameCreate {
            name: "f".into(),
            color: 0,
            template_type: "substr".into(),
            template: "flag{".into(),
            direction: "both".into(),
            special_type: None,
            action: None,
            enabled: true,
            ip_network: None,
            ip_endpoint: None,
            expression_source: None,
            service_targets_global: true,
            agent_targets_global: true,
            service_ids: vec![],
            agent_ids: vec![],
            threshold_count: None,
            threshold_window_secs: None,
            threshold_group_by: None,
            threshold_cooldown_secs: None,
            threshold_block_secs: None,
            expires_at: None,
        }
    }

    #[test]
    fn valid_substr_passes() {
        assert!(validate(&base(), "tag").is_ok());
    }

    #[test]
    fn legacy_special_type_maps_onto_action() {
        let mut t = base();
        t.special_type = Some("block_conn".into());
        assert_eq!(t.resolved_action(), "block_connection");
        assert!(validate(&t, &t.resolved_action()).is_ok());
    }

    #[test]
    fn threshold_on_tag_action_is_rejected() {
        let mut t = base();
        t.threshold_count = Some(5);
        assert!(validate(&t, "tag").is_err());
        assert!(validate(&t, "flag").is_err());
    }

    #[test]
    fn threshold_on_block_requires_all_fields() {
        let mut t = base();
        t.threshold_count = Some(5);
        assert!(validate(&t, "block").is_err());

        t.threshold_window_secs = Some(10);
        t.threshold_group_by = Some("remote_ip".into());
        t.threshold_cooldown_secs = Some(30);
        t.threshold_block_secs = Some(60);
        assert!(validate(&t, "block").is_ok());

        t.threshold_block_secs = None;
        assert!(validate(&t, "alert").is_ok());
        assert!(validate(&t, "block").is_err());
    }

    #[test]
    fn bsubstr_rejects_invalid_hex() {
        let mut t = base();
        t.template_type = "bsubstr".into();
        t.template = "zz-not-hex".into();
        assert!(validate(&t, "tag").is_err());

        t.template = "deadbeef".into();
        assert!(validate(&t, "tag").is_ok());
    }

    #[test]
    fn ip_cidr_requires_endpoint_and_network() {
        let mut t = base();
        t.template_type = "ip_cidr".into();
        t.template = String::new();
        assert!(validate(&t, "block").is_err());

        t.ip_network = Some("10.0.0.9".into());
        assert!(validate(&t, "block").is_err());

        t.ip_endpoint = Some("src".into());
        assert!(validate(&t, "block").is_ok());
    }

    #[test]
    fn cidr_normalization_masks_host_bits_and_expands_single_addresses() {
        let net = normalize_cidr("10.1.2.3/8").unwrap();
        assert_eq!(net.to_string(), "10.0.0.0/8");

        let host = normalize_cidr("10.1.2.3").unwrap();
        assert_eq!(host.to_string(), "10.1.2.3/32");

        let host6 = normalize_cidr("fd00::5").unwrap();
        assert_eq!(host6.to_string(), "fd00::5/128");

        let net6 = normalize_cidr("fd00::5/64").unwrap();
        assert_eq!(net6.to_string(), "fd00::/64");

        assert!(normalize_cidr("banana").is_err());
    }

    #[test]
    fn expression_requires_source() {
        let mut t = base();
        t.template_type = "expression".into();
        t.template = String::new();
        assert!(validate(&t, "tag").is_err());

        t.expression_source = Some("protocol == \"TCP\"".into());
        assert!(validate(&t, "tag").is_ok());
    }

    /// Expressions must compile through the shared DSL with a boolean
    /// result: type errors and unknown fields are rejected before
    /// persistence.
    #[test]
    fn expression_source_must_compile_to_bool() {
        let mut t = base();
        t.template_type = "expression".into();
        t.template = String::new();

        t.expression_source = Some("src_port == \"80\"".into());
        assert!(validate(&t, "tag").is_err());

        t.expression_source = Some("unknown_field == 1".into());
        assert!(validate(&t, "tag").is_err());

        t.expression_source = Some("src_ip".into());
        assert!(validate(&t, "tag").is_err());

        t.expression_source = Some("ip_in_cidr(remote_ip, \"10.0.0.0/8\")".into());
        assert!(validate(&t, "tag").is_ok());
    }

    /// Regex sources must compile within the compiled-size limit.
    #[test]
    fn regex_must_compile() {
        let mut t = base();
        t.template_type = "regex".into();
        t.template = "(unclosed".into();
        assert!(validate(&t, "tag").is_err());

        t.template = "flag\\{[a-z]+\\}".into();
        assert!(validate(&t, "tag").is_ok());
    }

    #[test]
    fn payload_filter_rejects_ip_fields() {
        let mut t = base();
        t.ip_network = Some("10.0.0.0/8".into());
        assert!(validate(&t, "tag").is_err());
    }
}
