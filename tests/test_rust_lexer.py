"""Direct static checks for the shared Rust lexical/module-tree authority."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import rust_lexer  # noqa: E402
import rust_module_tree  # noqa: E402


class RustLexerStaticTests(unittest.TestCase):
    def test_cfg_parser_preserves_supported_shapes_and_rejects_incomplete_input(self) -> None:
        self.assertEqual(
            rust_lexer._parse_cfg_expression('all(feature = "x", not(test))'),
            ("all", (("atom", 'feature="x"'), ("not", (("atom", "test"),)))),
        )
        for expression in ("not()", "all(feature", "bad-op(feature)", "feature = 1"):
            with self.assertRaises(SystemExit):
                rust_lexer._parse_cfg_expression(expression)

    def test_macro_meta_templates_are_found_only_with_a_matching_binding(self) -> None:
        source = (
            "macro_rules! attrs {\n"
            "    (#[$meta:meta] $item:item) => {\n"
            "        #[$meta]\n"
            "        $item\n"
            "    };\n"
            "}\n"
        )
        mask = rust_lexer._rust_code_mask(source)
        self.assertEqual(
            rust_lexer._macro_rule_template_attribute_starts(mask),
            {mask.index("#[$meta:meta]"), mask.index("#[$meta]", mask.index("=>"))},
        )

    def test_module_tree_output_keeps_production_and_test_views_distinct(self) -> None:
        with TemporaryDirectory(prefix="eg-lexer-static-") as directory:
            root = Path(directory)
            (root / "src" / "facade").mkdir(parents=True)
            (root / "src" / "facade.rs").write_text(
                '#[cfg_attr(feature = "x", derive(schemars::JsonSchema))]\n'
                "pub mod child;\n"
                "#[cfg(test)]\nmod tests { pub mod hidden; }\n"
                'include!("included.rs");\n',
                encoding="utf-8",
            )
            (root / "src" / "facade" / "child.rs").write_text(
                'pub const CHILD: &str = "child";\n', encoding="utf-8"
            )
            (root / "src" / "included.rs").write_text(
                'pub const INCLUDED: &str = "included";\n', encoding="utf-8"
            )
            (root / "src" / "facade" / "tests").mkdir()
            (root / "src" / "facade" / "tests" / "hidden.rs").write_text(
                'pub const HIDDEN: &str = "hidden";\n', encoding="utf-8"
            )

            production = rust_module_tree.read_module_tree(
                "src/facade.rs", root_dir=root
            )
            with_tests = rust_module_tree.read_module_tree(
                "src/facade.rs", root_dir=root, include_tests=True
            )

            self.assertIn("CHILD", production)
            self.assertIn("INCLUDED", production)
            self.assertNotIn("HIDDEN", production)
            self.assertIn("HIDDEN", with_tests)


if __name__ == "__main__":
    unittest.main()
