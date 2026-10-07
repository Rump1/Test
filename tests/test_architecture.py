"""Enforce dependency direction rather than a preferred directory spelling alone."""

import ast
import sys
from pathlib import Path

import pytest

APP = Path(__file__).resolve().parents[1] / "app"


@pytest.mark.parametrize(
    "layer,allowed,stdlib_only",
    [
        ("domain", ("app.domain",), True),
        ("application", ("app.domain", "app.application"), True),
        ("infrastructure", ("app.domain", "app.application", "app.infrastructure"), False),
        ("presentation", ("app.domain", "app.application", "app.presentation"), False),
    ],
)
def test_dependencies_point_inward(layer, allowed, stdlib_only):
    for path in (APP / layer).rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Import):
                imports = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                assert not node.level, f"Use explicit dependency paths: {path}:{node.lineno}"
                imports = [node.module or ""]
            else:
                continue
            for name in imports:
                if name.startswith("app."):
                    assert any(
                        name == prefix or name.startswith(prefix + ".") for prefix in allowed
                    ), f"{path}:{node.lineno}: forbidden dependency {name}"
                elif stdlib_only:
                    assert name.split(".")[0] in sys.stdlib_module_names, (
                        f"{path}:{node.lineno}: inner layer depends on {name}"
                    )
