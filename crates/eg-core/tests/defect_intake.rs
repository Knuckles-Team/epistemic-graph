//! Defect intake (EG-SDLC-R003.2).

use eg_core::lifecycle::defect::{normalize_symptom, signature_digest, DefectBook, FileOutcome};
use eg_types::lifecycle::{DefectIntake, DefectStatus, Symptom};

fn symptom(text: &str, at: u64) -> Symptom {
    Symptom {
        intake: DefectIntake::GateFailure,
        source_record_id: format!("gv-{at}"),
        observed_at: at,
        text: text.into(),
    }
}

// spec: EG-SDLC-R003.2
#[test]
fn same_signature_reuses_open_defect() {
    let a = "panic at src/a.rs:42:7 addr 0x7ffd1234 in /tmp/build-9z/out.log (line 12)";
    let b = "panic at src/a.rs:99:1 addr 0xdeadbeef in /tmp/build-1q/out.log (line 800)";
    assert_eq!(normalize_symptom(a), normalize_symptom(b));
    assert_eq!(signature_digest(a), signature_digest(b));
    assert_ne!(signature_digest(a), signature_digest("a different failure"));
    assert!(!normalize_symptom(a).contains("42") && !normalize_symptom(a).contains("0x7ffd"));

    let mut book = DefectBook::default();
    let first = book.file(
        "medulla",
        DefectIntake::GateFailure,
        "panic",
        None,
        symptom(a, 1),
    );
    let FileOutcome::Created(id) = first else {
        panic!("first symptom creates")
    };
    let second = book.file(
        "medulla",
        DefectIntake::RunFailure,
        "panic",
        None,
        symptom(b, 2),
    );
    assert_eq!(second, FileOutcome::Absorbed(id.clone()));
    assert_eq!(book.defects().len(), 1);
    assert_eq!(book.defects()[0].symptoms.len(), 2);

    // A different signature is a different defect.
    let other = book.file(
        "medulla",
        DefectIntake::Direct,
        "other",
        None,
        symptom("disk full", 3),
    );
    assert!(matches!(other, FileOutcome::Created(_)));
    assert_eq!(book.defects().len(), 2);

    // A closed defect with the same signature reopens, with the new symptom.
    book.set_status(&id, DefectStatus::Closed);
    let again = book.file(
        "medulla",
        DefectIntake::Incident,
        "panic",
        None,
        symptom(a, 4),
    );
    assert_eq!(again, FileOutcome::Reopened(id.clone()));
    assert_eq!(book.defects()[0].status, DefectStatus::New);
    assert_eq!(book.defects()[0].symptoms.len(), 3);
    assert_eq!(book.defects().len(), 2);
}
