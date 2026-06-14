use super::*;

#[test]
fn oversized_catalog_page_is_valid_json_within_budget_and_has_cursor() {
    let skills = (0..20)
        .map(|index| ListedSkill {
            authority: SkillToolAuthority::Orchestrator,
            package: format!("package-{index}"),
            name: format!("skill-{index}"),
            description: "description ".repeat(100),
            main_resource: format!("skill://package-{index}/SKILL.md"),
        })
        .collect::<Vec<_>>();
    let max_response_bytes = 12_000;

    let response = page_response(&skills, 0, Vec::new(), max_response_bytes)
        .expect("oversized catalog should be paginated");
    let serialized = serde_json::to_vec(&response).expect("serialize list response");
    let decoded: serde_json::Value =
        serde_json::from_slice(&serialized).expect("list response should remain valid JSON");

    assert!(serialized.len() <= max_response_bytes);
    assert!(decoded["next_cursor"].is_string());
    assert!(response.skills.len() < skills.len());

    let next_start =
        parse_pagination_cursor(response.next_cursor.as_deref(), &skills, "skills.list")
            .expect("parse list cursor");
    let next_response = page_response(&skills, next_start, Vec::new(), max_response_bytes)
        .expect("list next catalog page");
    let next_serialized = serde_json::to_vec(&next_response).expect("serialize next list page");
    let _: serde_json::Value =
        serde_json::from_slice(&next_serialized).expect("next list page should be valid JSON");
    assert!(next_serialized.len() <= max_response_bytes);
    assert!(!next_response.skills.is_empty());
}

#[test]
fn single_entry_bound_uses_effective_model_budget() {
    let skill = ListedSkill {
        authority: SkillToolAuthority::Orchestrator,
        package: "package".to_string(),
        name: "skill".to_string(),
        description: "description ".repeat(100),
        main_resource: "skill://package/SKILL.md".to_string(),
    };

    assert!(!single_entry_response_is_bounded(&skill, 128));
    assert!(single_entry_response_is_bounded(
        &skill,
        MAX_LIST_RESPONSE_BYTES
    ));
}
