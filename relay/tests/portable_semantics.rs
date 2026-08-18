use std::collections::BTreeSet;

use serde_json::Value;

const VECTORS: &str = include_str!("../../../protocol/conformance/connector/v1/vectors.json");

fn positive(value: Option<&Value>) -> bool {
    value.and_then(Value::as_u64).is_some_and(|value| value > 0)
}

fn validate(case: &Value) -> Result<(), &'static str> {
    if let Some(status) = case.get("service_status") {
        if matches!(
            status["delivery_readiness"].as_str(),
            Some("not_ready" | "ready")
        ) && matches!(
            status["health"].as_str(),
            Some("healthy" | "degraded" | "unhealthy")
        ) && matches!(status["recovery"].as_str(), Some("idle" | "reconnecting"))
            && positive(status.get("revision"))
        {
            return Ok(());
        }
        return Err("connector.status.invalid");
    }

    let manifest = case.get("manifest").ok_or("connector.manifest.missing")?;
    if manifest["api_revision"].as_u64() != Some(1) || !positive(manifest.get("manifest_revision"))
    {
        return Err("connector.manifest.invalid_revision");
    }
    if manifest["package_id"].as_str().is_none_or(str::is_empty)
        || manifest["package_version"]
            .as_str()
            .is_none_or(str::is_empty)
    {
        return Err("connector.manifest.invalid_identity");
    }
    let fields = manifest["configuration"]["fields"]
        .as_array()
        .ok_or("connector.configuration.invalid_fields")?;
    let mut field_names = BTreeSet::new();
    for field in fields {
        let name = field["name"]
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or("connector.configuration.invalid_field")?;
        if !field_names.insert(name) {
            return Err("connector.configuration.duplicate_field");
        }
        if field["kind"].as_str() == Some("secret") && field.get("default").is_some() {
            return Err("connector.configuration.secret_default_forbidden");
        }
    }
    let components = manifest["components"]
        .as_array()
        .ok_or("connector.manifest.invalid_components")?;
    let mut component_ids = BTreeSet::new();
    for component in components {
        let id = component["component_id"]
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or("connector.manifest.invalid_component")?;
        if !component_ids.insert(id) {
            return Err("connector.manifest.duplicate_component");
        }
        if !matches!(
            component["kind"].as_str(),
            Some("source" | "operator" | "endpoint")
        ) {
            return Err("connector.manifest.invalid_component_kind");
        }
        for port in component["ports"]
            .as_array()
            .ok_or("connector.manifest.invalid_ports")?
        {
            if port["name"].as_str().is_none_or(str::is_empty)
                || port["signal_spec_id"].as_str().is_none_or(str::is_empty)
                || !matches!(port["direction"].as_str(), Some("input" | "output"))
                || !matches!(port["multiplicity"].as_str(), Some("one" | "many"))
            {
                return Err("connector.manifest.invalid_port");
            }
        }
    }
    if let Some(endpoint) = case.get("endpoint") {
        if !positive(endpoint.get("maximum_inflight_items"))
            || !positive(endpoint.get("maximum_payload_bytes"))
            || !positive(endpoint.get("startup_deadline_ms"))
            || !positive(endpoint.get("shutdown_deadline_ms"))
            || !positive(endpoint.get("probe_interval_ms"))
            || !positive(endpoint.get("success_threshold"))
            || !positive(endpoint.get("failure_threshold"))
        {
            return Err("connector.endpoint.invalid_deadline");
        }
        if !components.iter().any(|component| {
            component["component_id"] == endpoint["component_id"]
                && component["kind"].as_str() == Some("endpoint")
        }) {
            return Err("connector.endpoint.unknown_component");
        }
    }
    Ok(())
}

#[test]
fn relay_package_consumes_canonical_connector_vectors() {
    let corpus: Value = serde_json::from_str(VECTORS).expect("canonical connector vectors");
    let cases = corpus["cases"].as_array().expect("finite vector cases");
    for case in cases {
        let result = validate(case);
        match case["expected"].as_str() {
            Some("accept") => assert!(result.is_ok(), "{}: {result:?}", case["id"]),
            Some("reject") => assert_eq!(
                result.expect_err("negative vector must reject"),
                case["error_code"].as_str().expect("stable rejection code"),
                "{}",
                case["id"]
            ),
            expectation => panic!("unknown vector expectation: {expectation:?}"),
        }
    }
}
