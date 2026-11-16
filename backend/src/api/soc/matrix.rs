// The configuration/failure matrix from the LLM-SOC spec, encoded as a
// pure function of the loaded JEV and SOC settings plus the JEV
// scheduler's observed run state. Every row of the spec table must be
// representable; the Settings UI renders the reason strings verbatim.

pub const ROW_JEV_OFF_SOC_OFF: &str = "jev_off_soc_off";
pub const ROW_JEV_OFF_SOC_ON: &str = "jev_off_soc_on";
pub const ROW_JEV_ON_SOC_OFF: &str = "jev_on_soc_off";
pub const ROW_JEV_ON_SOC_AUTO_OFF: &str = "jev_on_soc_auto_off";
pub const ROW_JEV_ON_SOC_AUTO_ON: &str = "jev_on_soc_auto_on";
pub const ROW_JEV_PROVIDER_FAILING_SOC_ON: &str = "jev_provider_failing_soc_on";

#[derive(Debug, Clone, serde::Serialize)]
pub struct Availability {
    pub available: bool,
    pub reason: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct MatrixState {
    pub jev_state: String,
    pub soc_state: String,
    pub matrix_row: String,
    pub manual: Availability,
    pub auto: Availability,
}

/// Inputs straight from the loaded settings; `jev_provider_failing` comes
/// from the JEV scheduler snapshot (run_state backoff/active-with-errors).
pub struct MatrixInput {
    pub jev_enabled: bool,
    pub jev_configured: bool,
    pub jev_provider_failing: bool,
    pub soc_enabled: bool,
    pub soc_auto_enabled: bool,
    pub claims_paused_reason: Option<String>,
}

pub fn effective_state(input: &MatrixInput) -> MatrixState {
    let jev_on = input.jev_enabled && input.jev_configured;
    let soc_on = input.soc_enabled;

    let (jev_state, matrix_row, manual, auto) = if !jev_on && !soc_on {
        (
            "unconfigured_or_disabled",
            ROW_JEV_OFF_SOC_OFF,
            Availability {
                available: false,
                reason: "manual triage unavailable: enable LLM-SOC (soc_enabled) or set up JEV; no analysis backend is active".to_string(),
            },
            Availability {
                available: false,
                reason: "automatic triage disabled: neither JEV nor LLM-SOC is active".to_string(),
            },
        )
    } else if !jev_on && soc_on {
        (
            "unconfigured_or_disabled",
            ROW_JEV_OFF_SOC_ON,
            Availability {
                available: true,
                reason: "manual triage available for any stream; no JEV evidence is attached"
                    .to_string(),
            },
            Availability {
                available: false,
                reason: "automatic triage disabled because the JEV trigger is absent".to_string(),
            },
        )
    } else if jev_on && !soc_on {
        (
            "configured_and_enabled",
            ROW_JEV_ON_SOC_OFF,
            Availability {
                available: false,
                reason: "manual triage unavailable: LLM-SOC is disabled; JEV verdicts continue"
                    .to_string(),
            },
            Availability {
                available: false,
                reason: "automatic triage disabled: LLM-SOC is off, no jobs are queued".to_string(),
            },
        )
    } else if input.jev_provider_failing {
        (
            "provider_failing",
            ROW_JEV_PROVIDER_FAILING_SOC_ON,
            Availability {
                available: true,
                reason: "manual triage available using stream data and existing JEV verdicts".to_string(),
            },
            Availability {
                available: true,
                reason: "automatic triage armed but idle: no new automatic jobs until new valid JEV verdicts arrive".to_string(),
            },
        )
    } else if input.soc_auto_enabled {
        (
            "configured_and_enabled",
            ROW_JEV_ON_SOC_AUTO_ON,
            Availability {
                available: true,
                reason: "manual triage available".to_string(),
            },
            Availability {
                available: true,
                reason: "qualifying new JEV verdicts enqueue automatic triage jobs".to_string(),
            },
        )
    } else {
        (
            "configured_and_enabled",
            ROW_JEV_ON_SOC_AUTO_OFF,
            Availability {
                available: true,
                reason: "manual triage available".to_string(),
            },
            Availability {
                available: false,
                reason: "manual only: soc_auto_enabled=false, existing queued jobs still run"
                    .to_string(),
            },
        )
    };

    // Outage overlays (matrix rows 7-9): reported as an effective pause on
    // claims without changing the trigger eligibility above.
    let auto = if let Some(pause) = &input.claims_paused_reason {
        if auto.available {
            Availability {
                available: false,
                reason: format!("{}; {}", auto.reason, pause),
            }
        } else {
            Availability {
                reason: format!("{}; {}", auto.reason, pause),
                ..auto
            }
        }
    } else {
        auto
    };

    MatrixState {
        jev_state: jev_state.to_string(),
        soc_state: if soc_on { "enabled" } else { "disabled" }.to_string(),
        matrix_row: matrix_row.to_string(),
        manual,
        auto,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(jev_on: bool, soc_on: bool, auto_on: bool) -> MatrixInput {
        MatrixInput {
            jev_enabled: jev_on,
            jev_configured: jev_on,
            jev_provider_failing: false,
            soc_enabled: soc_on,
            soc_auto_enabled: auto_on,
            claims_paused_reason: None,
        }
    }

    #[test]
    fn every_matrix_row_is_representable() {
        let rows = [
            // (row, input)
            (ROW_JEV_OFF_SOC_OFF, input(false, false, false)),
            (ROW_JEV_OFF_SOC_ON, input(false, true, true)),
            (ROW_JEV_ON_SOC_OFF, input(true, false, false)),
            (ROW_JEV_ON_SOC_AUTO_OFF, input(true, true, false)),
            (ROW_JEV_ON_SOC_AUTO_ON, input(true, true, true)),
        ];
        for (expected, i) in rows {
            assert_eq!(effective_state(&i).matrix_row, expected);
        }
        let mut failing = input(true, true, true);
        failing.jev_provider_failing = true;
        assert_eq!(
            effective_state(&failing).matrix_row,
            ROW_JEV_PROVIDER_FAILING_SOC_ON
        );
    }

    #[test]
    fn manual_needs_soc_only_and_auto_needs_both() {
        // Row 2: SOC without JEV -> manual works, automatic disabled.
        let s = effective_state(&input(false, true, true));
        assert!(s.manual.available);
        assert!(!s.auto.available);
        assert!(s.auto.reason.contains("JEV trigger"));

        // Row 3: JEV without SOC -> neither.
        let s = effective_state(&input(true, false, true));
        assert!(!s.manual.available);
        assert!(!s.auto.available);

        // Row 4: auto off keeps manual alive.
        let s = effective_state(&input(true, true, false));
        assert!(s.manual.available);
        assert!(!s.auto.available);
        assert!(s.auto.reason.contains("queued jobs still run"));
    }

    #[test]
    fn provider_failure_keeps_manual_and_waits_for_new_verdicts() {
        let mut i = input(true, true, true);
        i.jev_provider_failing = true;
        let s = effective_state(&i);
        assert!(s.manual.available);
        assert!(s.auto.available);
        assert!(s.auto.reason.contains("new valid JEV verdicts"));
    }

    #[test]
    fn outage_overlay_pauses_claims_without_silence() {
        let mut i = input(true, true, true);
        i.claims_paused_reason = Some("daily job limit reached".to_string());
        let s = effective_state(&i);
        assert!(!s.auto.available);
        assert!(s.auto.reason.contains("daily job limit"));
        // A disabled trigger keeps its own reason first, overlay appended.
        let mut j = input(true, true, false);
        j.claims_paused_reason = Some("daily job limit reached".to_string());
        let s = effective_state(&j);
        assert!(s.auto.reason.contains("manual only"));
        assert!(s.auto.reason.contains("daily job limit"));
    }
}
