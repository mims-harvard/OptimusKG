"""Tests for the README performance-section rewriter.

``make mcpb-profile`` edits README.md in place, so the rewrite must replace
exactly the performance section, leave every other section intact, and be
idempotent. Getting this wrong would either duplicate the table or silently
truncate the rest of the README.
"""

import pytest
from optimuskg_mcp.profiling import NEXT_HEADING, SECTION_HEADING, update_readme

README = f"""# Title

Intro paragraph.

## Install

Install steps.

{SECTION_HEADING}

Old preamble.

| Tool | Median (ms) |
| --- | ---: |
| `list_schema` | 99.99 |

_Environment: old machine._

{NEXT_HEADING}

Development notes.

## License

MIT.
"""

SECTION = f"""{SECTION_HEADING}

New preamble.

| Tool | Median (ms) |
| --- | ---: |
| `list_schema` | 11.11 |

_Environment: new machine._
"""


def test_replaces_the_numbers():
    out = update_readme(README, SECTION)
    assert "11.11" in out
    assert "99.99" not in out
    assert "old machine" not in out


def test_keeps_surrounding_sections():
    out = update_readme(README, SECTION)
    for kept in ("# Title", "Intro paragraph.", "## Install", "Install steps."):
        assert kept in out
    for kept in (NEXT_HEADING, "Development notes.", "## License", "MIT."):
        assert kept in out


def test_does_not_duplicate_the_section():
    out = update_readme(README, SECTION)
    assert out.count(SECTION_HEADING) == 1
    assert out.count(NEXT_HEADING) == 1


def test_is_idempotent():
    once = update_readme(README, SECTION)
    assert update_readme(once, SECTION) == once


def test_next_heading_stays_separated():
    out = update_readme(README, SECTION)
    # A blank line must remain before the following heading.
    assert f"_Environment: new machine._\n\n{NEXT_HEADING}" in out


def test_raises_when_the_section_is_missing():
    # Better to fail loudly than to append a second, contradictory table.
    with pytest.raises(ValueError, match="Could not find"):
        update_readme("# Title\n\n## Development\n\nNotes.\n", SECTION)


def test_raises_when_the_following_heading_is_missing():
    truncated = f"# Title\n\n{SECTION_HEADING}\n\nTable goes here.\n"
    with pytest.raises(ValueError, match="Could not find"):
        update_readme(truncated, SECTION)


def test_real_readme_has_the_expected_anchors():
    from optimuskg_mcp.profiling import README as readme_path

    text = readme_path.read_text()
    assert text.count(SECTION_HEADING) == 1
    assert text.count(NEXT_HEADING) == 1
    assert text.index(SECTION_HEADING) < text.index(NEXT_HEADING)
