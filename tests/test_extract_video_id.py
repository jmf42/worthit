import os
import ast
import types
import functools
import re
import pytest


def load_extract_video_id():
    """Load extract_video_id from app.py without importing heavy dependencies."""
    path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "app.py")
    with open(path, "r") as f:
        source = f.read()

    tree = ast.parse(source, filename=path)
    module = types.ModuleType("app_partial")
    module.re = re
    module.lru_cache = functools.lru_cache
    module.logger = types.SimpleNamespace(warning=lambda *args, **kwargs: None)

    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "VIDEO_ID_REGEX":
                    exec(compile(ast.Module([node], []), path, "exec"), module.__dict__)
        elif isinstance(node, ast.FunctionDef) and node.name in ("validate_video_id", "extract_video_id"):
            exec(compile(ast.Module([node], []), path, "exec"), module.__dict__)

    return module.extract_video_id


extract_video_id = load_extract_video_id()

# Valid video IDs and URLs
@pytest.mark.parametrize("input_value, expected", [
    ("dQw4w9WgXcQ", "dQw4w9WgXcQ"),
    ("https://www.youtube.com/watch?v=dQw4w9WgXcQ", "dQw4w9WgXcQ"),
    ("https://youtu.be/dQw4w9WgXcQ", "dQw4w9WgXcQ"),
    ("https://www.youtube.com/shorts/dQw4w9WgXcQ", "dQw4w9WgXcQ"),
    ("https://www.youtube.com/embed/dQw4w9WgXcQ", "dQw4w9WgXcQ"),
    ("https://www.youtube.com/live/dQw4w9WgXcQ?feature=share", "dQw4w9WgXcQ"),
])
def test_extract_video_id_valid(input_value, expected):
    assert extract_video_id(input_value) == expected


# Invalid IDs or URLs should return None
@pytest.mark.parametrize("input_value", [
    "shortid",
    "1234567890",  # 10 chars
    "invalid!chars",
    "https://www.youtube.com/watch?v=invalid",
    "https://youtu.be/dQw4w9",
    "https://www.youtube.com/shorts/123456789",
    "some random text",
    "",
])
def test_extract_video_id_invalid(input_value):
    assert extract_video_id(input_value) is None
