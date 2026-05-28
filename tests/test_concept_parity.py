import re
from pathlib import Path


def test_concept_parity():
    """Ensure all CONCEPT:ID tags used in source code are registered in docs/concepts.md."""
    concepts_file = Path(__file__).parent.parent / "docs" / "concepts.md"
    assert concepts_file.exists(), "docs/concepts.md is missing!"

    with open(concepts_file, encoding="utf-8") as f:
        concepts_content = f.read()

    root_dir = Path(__file__).parent.parent
    used_concepts = set()

    # Scan Rust source files
    for rs_file in (root_dir / "src").glob("**/*.rs"):
        with open(rs_file, encoding="utf-8") as f:
            content = f.read()
            found = re.findall(r"CONCEPT:[A-Z]+-\d+(?:\.\d+)?", content)
            used_concepts.update(found)

    # Assert that each used concept is present in the concepts registry
    for concept in used_concepts:
        assert concept in concepts_content, (
            f"Concept {concept} is used in source code but not registered in docs/concepts.md!"
        )
