//! Skill supporting files (ruling D11, EH-094).
//!
//! A skill is more than its `SKILL.md`: the scripts, references and assets it
//! names by relative path are part of what an agent runs. Each travels as one
//! `skill_file` entry whose name is `<skill>/<relative path>`, whose URI is
//! `skill-file://<name>`, and which references its skill's `SKILL.md` -- so the
//! file cannot be imported without its skill (G17 resolves the reference) and
//! withdraws with it. Its body may be any bytes; it is bounded like every body.
//!
//! The path is refused unless every segment is a plain name: no `..`, no
//! absolute path, no separator but `/`, nothing a consumer could resolve
//! outside the skill's own directory.

use eg_types::connector_pack::{PackEntry, PackEntryKind, PackRef};

/// Check one skill-file entry's identity and its reference to its skill.
/// Every other kind passes untouched.
pub(super) fn validate_skill_file(entry: &PackEntry) -> Result<(), &'static str> {
    if entry.kind != PackEntryKind::SkillFile {
        return Ok(());
    }
    let (skill, path) = entry
        .name
        .split_once('/')
        .ok_or("skill file name must be <skill>/<relative path>")?;
    if !plain_segment(skill) || !path.split('/').all(plain_segment) {
        return Err("skill file path must be relative plain-name segments only");
    }
    if entry.uri != format!("skill-file://{}", entry.name) {
        return Err("skill file URI must be skill-file://<skill>/<relative path>");
    }
    let expected = PackRef {
        uri: format!("skill://{skill}/SKILL.md"),
        kind: PackEntryKind::Skill,
    };
    if !entry
        .references
        .iter()
        .any(|reference| *reference == expected)
    {
        return Err("skill file must reference its skill's SKILL.md");
    }
    Ok(())
}

fn plain_segment(segment: &str) -> bool {
    !segment.is_empty()
        && segment != "."
        && segment != ".."
        && segment
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(name: &str, uri: &str, references: Vec<PackRef>) -> PackEntry {
        let mut entry =
            eg_types::test_support::contract_wave::pack::entry(PackEntryKind::SkillFile, name);
        entry.uri = uri.to_string();
        entry.references = eg_types::contract::BoundedVec::new(references).unwrap();
        entry
    }

    fn skill_ref(skill: &str) -> PackRef {
        PackRef {
            uri: format!("skill://{skill}/SKILL.md"),
            kind: PackEntryKind::Skill,
        }
    }

    #[test]
    fn a_nested_supporting_file_of_its_skill_is_accepted() {
        let ok = entry(
            "deploy/scripts/run.sh",
            "skill-file://deploy/scripts/run.sh",
            vec![skill_ref("deploy")],
        );
        assert_eq!(validate_skill_file(&ok), Ok(()));
    }

    #[test]
    fn traversal_absolute_and_foreign_skill_files_are_refused() {
        for (name, uri, skill) in [
            (
                "deploy/../secrets",
                "skill-file://deploy/../secrets",
                "deploy",
            ),
            ("deploy//run.sh", "skill-file://deploy//run.sh", "deploy"),
            ("deploy", "skill-file://deploy", "deploy"),
            ("deploy/run.sh", "skill-file://other/run.sh", "deploy"),
            ("deploy/run.sh", "skill-file://deploy/run.sh", "other"),
            ("deploy/a b", "skill-file://deploy/a b", "deploy"),
        ] {
            let bad = entry(name, uri, vec![skill_ref(skill)]);
            assert!(validate_skill_file(&bad).is_err(), "{name} {uri} {skill}");
        }
    }
}
