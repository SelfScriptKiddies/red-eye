use serde::Serialize;
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;

use super::openrouter::{ATTACK_CLASSES, DecisionsResponse, NOUL_QUESTIONS, parse_response};

/// Fully validated answer set. A value that failed validation never
/// becomes a verdict: missing or out-of-range answers are an INVALID
/// response, not a benign one.
#[derive(Debug, Clone, Serialize)]
pub struct ValidatedAnswers {
    /// P(yes) per noul question, all finite and within 0..=1.
    pub nouls: BTreeMap<String, f64>,
    pub attack_class: String,
    pub choice_confidence: f64,
    /// Level probabilities keyed by "0".."3" when the provider sent them.
    pub severity: f64,
    pub severity_confidence: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Decision {
    pub is_attack: bool,
    pub attack_class: String,
    pub confidence: f64,
    pub severity: f64,
    /// Highest noul probability, for the WS payload and the UI badge.
    pub top_nouls: Vec<(String, f64)>,
    /// True when the rule fired on the choice branch rather than a noul.
    pub via_choice: bool,
}

pub fn validate_response(raw: Value) -> Result<(DecisionsResponse, ValidatedAnswers), String> {
    let response = parse_response(raw)?;
    let answers = validate_answers(&response.answers)?;
    Ok((response, answers))
}

fn probability(v: &Value, name: &str) -> Result<f64, String> {
    let n = v
        .as_f64()
        .ok_or_else(|| format!("answer '{}' is not a number", name))?;
    if !n.is_finite() || !(0.0..=1.0).contains(&n) {
        return Err(format!(
            "answer '{}' probability {} out of range 0..1",
            name, n
        ));
    }
    Ok(n)
}

pub fn validate_answers(answers: &BTreeMap<String, Value>) -> Result<ValidatedAnswers, String> {
    let mut nouls = BTreeMap::new();
    for name in NOUL_QUESTIONS {
        let v = answers
            .get(name)
            .ok_or_else(|| format!("missing required noul answer '{}'", name))?;
        let p = v
            .get("noul")
            .ok_or_else(|| format!("noul answer '{}' misses field 'noul'", name))?;
        nouls.insert((*name).to_string(), probability(p, name)?);
    }

    let choice = answers
        .get("attack_class")
        .ok_or_else(|| "missing required choice answer 'attack_class'".to_string())?;
    let attack_class = choice
        .get("choice")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "choice answer 'attack_class' misses string field 'choice'".to_string())?
        .to_string();
    if !ATTACK_CLASSES.contains(&attack_class.as_str()) {
        return Err(format!("unknown attack_class '{}'", attack_class));
    }
    let choice_confidence = probability(
        choice
            .get("confidence")
            .ok_or_else(|| "choice answer 'attack_class' misses 'confidence'".to_string())?,
        "attack_class.confidence",
    )?;

    let score = answers
        .get("severity")
        .ok_or_else(|| "missing required score answer 'severity'".to_string())?;
    let severity = score
        .get("score")
        .and_then(|v| v.as_f64())
        .ok_or_else(|| "score answer 'severity' misses numeric field 'score'".to_string())?;
    if !severity.is_finite() || !(0.0..=3.0).contains(&severity) {
        return Err(format!("severity score {} out of range 0..3", severity));
    }
    let severity_confidence = probability(
        score
            .get("confidence")
            .ok_or_else(|| "score answer 'severity' misses 'confidence'".to_string())?,
        "severity.confidence",
    )?;

    Ok(ValidatedAnswers {
        nouls,
        attack_class,
        choice_confidence,
        severity,
        severity_confidence,
    })
}

/// The decision rule runs in CODE, never in the model:
///   is_attack = max(noul probabilities) >= threshold
///            OR (attack_class != "benign" AND choice confidence >= threshold)
pub fn decide(valid: &ValidatedAnswers, threshold: f64) -> Decision {
    let mut top: Vec<(String, f64)> = valid.nouls.iter().map(|(k, v)| (k.clone(), *v)).collect();
    // Descending, name-ascending tiebreak: deterministic for the same
    // answers regardless of map iteration order.
    top.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.0.cmp(&b.0))
    });

    let max_noul = top.first().map(|(_, p)| *p).unwrap_or(0.0);
    let via_choice = valid.attack_class != "benign" && valid.choice_confidence >= threshold;
    let is_attack = max_noul >= threshold || via_choice;

    Decision {
        is_attack,
        attack_class: valid.attack_class.clone(),
        confidence: valid.choice_confidence,
        severity: valid.severity,
        top_nouls: top.into_iter().take(3).collect(),
        via_choice,
    }
}

/// The complete validated answer map stored with the verdict (JSONB):
/// nouls + choice + score, everything the provider returned that passed
/// validation. Nothing else from the response is persisted.
pub fn answers_json(valid: &ValidatedAnswers) -> Value {
    let mut nouls = Map::new();
    for (k, v) in &valid.nouls {
        nouls.insert(k.clone(), json!(v));
    }
    json!({
        "nouls": nouls,
        "attack_class": {
            "choice": valid.attack_class,
            "confidence": valid.choice_confidence,
        },
        "severity": {
            "score": valid.severity,
            "confidence": valid.severity_confidence,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn valid_answers() -> BTreeMap<String, Value> {
        let mut answers = BTreeMap::new();
        for (i, name) in NOUL_QUESTIONS.iter().enumerate() {
            answers.insert(
                name.to_string(),
                json!({"type": "noul", "noul": 0.01 * (i as f64)}),
            );
        }
        answers.insert(
            "attack_class".to_string(),
            json!({"type": "choice", "choice": "benign", "confidence": 0.9,
                   "probabilities": {"benign": 0.9, "sqli": 0.05}}),
        );
        answers.insert(
            "severity".to_string(),
            json!({"type": "score", "score": 0.5, "confidence": 0.9,
                   "probabilities": {"0": 0.8, "1": 0.2}}),
        );
        answers
    }

    #[test]
    fn all_required_answers_validate() {
        let (raw, _) = validate_response(json!({"answers": valid_answers()})).expect("valid");
        assert_eq!(raw.answers.len(), 9);
    }

    #[test]
    fn missing_noul_is_invalid_not_benign() {
        let mut answers = valid_answers();
        answers.remove("is_exfiltration");
        let err = validate_answers(&answers).unwrap_err();
        assert!(err.contains("is_exfiltration"), "{}", err);
    }

    #[test]
    fn missing_choice_or_score_is_invalid() {
        let mut answers = valid_answers();
        answers.remove("attack_class");
        assert!(validate_answers(&answers).is_err());
        let mut answers = valid_answers();
        answers.remove("severity");
        assert!(validate_answers(&answers).is_err());
    }

    #[test]
    fn out_of_range_probabilities_are_invalid() {
        let mut answers = valid_answers();
        answers.insert("is_xss".to_string(), json!({"type": "noul", "noul": 1.5}));
        let err = validate_answers(&answers).unwrap_err();
        assert!(err.contains("out of range"), "{}", err);
        let mut answers = valid_answers();
        answers.insert(
            "is_xss".to_string(),
            json!({"type": "noul", "noul": "high"}),
        );
        assert!(validate_answers(&answers).is_err());
    }

    #[test]
    fn unknown_choice_class_is_invalid() {
        let mut answers = valid_answers();
        answers.insert(
            "attack_class".to_string(),
            json!({"type": "choice", "choice": "rootkit", "confidence": 0.9}),
        );
        let err = validate_answers(&answers).unwrap_err();
        assert!(err.contains("unknown attack_class"), "{}", err);
    }

    #[test]
    fn severity_bounds_enforced() {
        let mut answers = valid_answers();
        answers.insert(
            "severity".to_string(),
            json!({"type": "score", "score": 3.5, "confidence": 0.9}),
        );
        assert!(validate_answers(&answers).is_err());
    }

    #[test]
    fn decision_at_threshold_is_attack() {
        let mut answers = valid_answers();
        answers.insert(
            "is_sql_injection".to_string(),
            json!({"type": "noul", "noul": 0.7}),
        );
        let valid = validate_answers(&answers).unwrap();
        let d = decide(&valid, 0.7);
        assert!(d.is_attack);
        assert!(!d.via_choice);
        assert_eq!(d.top_nouls[0].0, "is_sql_injection");
    }

    #[test]
    fn decision_above_threshold_is_attack() {
        let mut answers = valid_answers();
        answers.insert(
            "is_sql_injection".to_string(),
            json!({"type": "noul", "noul": 0.93}),
        );
        let valid = validate_answers(&answers).unwrap();
        let d = decide(&valid, 0.7);
        assert!(d.is_attack);
        assert_eq!(d.attack_class, "benign");
    }

    #[test]
    fn decision_below_threshold_is_benign() {
        let valid = validate_answers(&valid_answers()).unwrap();
        let d = decide(&valid, 0.7);
        assert!(!d.is_attack);
    }

    #[test]
    fn decision_choice_branch_fires_without_noul() {
        let mut answers = valid_answers();
        answers.insert(
            "attack_class".to_string(),
            json!({"type": "choice", "choice": "sqli", "confidence": 0.88}),
        );
        let valid = validate_answers(&answers).unwrap();
        let d = decide(&valid, 0.7);
        assert!(d.is_attack);
        assert!(d.via_choice);
    }

    #[test]
    fn benign_choice_with_high_confidence_is_not_attack() {
        let mut answers = valid_answers();
        answers.insert(
            "attack_class".to_string(),
            json!({"type": "choice", "choice": "benign", "confidence": 0.99}),
        );
        let valid = validate_answers(&answers).unwrap();
        assert!(!decide(&valid, 0.7).is_attack);
    }

    #[test]
    fn choice_branch_below_threshold_is_not_attack() {
        let mut answers = valid_answers();
        answers.insert(
            "attack_class".to_string(),
            json!({"type": "choice", "choice": "sqli", "confidence": 0.69}),
        );
        let valid = validate_answers(&answers).unwrap();
        assert!(!decide(&valid, 0.7).is_attack);
    }

    #[test]
    fn answers_json_stores_complete_validated_map() {
        let valid = validate_answers(&valid_answers()).unwrap();
        let stored = answers_json(&valid);
        assert_eq!(stored["nouls"].as_object().unwrap().len(), 7);
        assert_eq!(stored["attack_class"]["choice"], "benign");
        assert_eq!(stored["severity"]["score"], 0.5);
    }

    #[test]
    fn top_nouls_are_three_sorted_descending() {
        let mut answers = valid_answers();
        answers.insert("is_xss".to_string(), json!({"type": "noul", "noul": 0.6}));
        answers.insert(
            "is_sql_injection".to_string(),
            json!({"type": "noul", "noul": 0.9}),
        );
        answers.insert(
            "is_exfiltration".to_string(),
            json!({"type": "noul", "noul": 0.75}),
        );
        let valid = validate_answers(&answers).unwrap();
        let d = decide(&valid, 0.5);
        let tops: Vec<f64> = d.top_nouls.iter().map(|(_, p)| *p).collect();
        assert_eq!(tops.len().min(3), d.top_nouls.len());
        assert!(tops.windows(2).all(|w| w[0] >= w[1]));
        assert_eq!(d.top_nouls.len(), 3);
    }
}
