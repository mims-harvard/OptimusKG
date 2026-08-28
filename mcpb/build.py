#!/usr/bin/env python3
"""Validate and pack the OptimusKG MCP bundle into ``optimuskg.mcpb``."""

from __future__ import annotations

import json
import shutil
import subprocess
import tomllib
from pathlib import Path

import typer

BUNDLE_ROOT = Path(__file__).resolve().parent
MANIFEST = BUNDLE_ROOT / "manifest.json"
PYPROJECT = BUNDLE_ROOT / "pyproject.toml"
OUTPUT = BUNDLE_ROOT / "dist" / "optimuskg.mcpb"


def _fail(msg: str) -> None:
    typer.secho(f"ERROR: {msg}", fg=typer.colors.RED, err=True)
    raise typer.Exit(code=1)


def validate() -> dict:
    manifest = json.loads(MANIFEST.read_text())
    pyproject = tomllib.loads(PYPROJECT.read_text())

    m_version = manifest.get("version")
    p_version = pyproject["project"]["version"]
    if m_version != p_version:
        _fail(
            f"Version mismatch: manifest {m_version!r} != pyproject {p_version!r}. "
            "Bump both together for each release."
        )

    for field in ("name", "server", "version"):
        if field not in manifest:
            _fail(f"manifest.json is missing required field '{field}'.")

    server = manifest["server"]
    entry = BUNDLE_ROOT / server["entry_point"]
    if not entry.is_file():
        _fail(f"entry_point does not exist: {entry}")

    doi = server.get("mcp_config", {}).get("env", {}).get("OPTIMUSKG_DOI", "")
    if not doi:
        _fail(
            "OPTIMUSKG_DOI is not pinned in manifest env; pin a Dataverse DOI "
            "so each release queries a reproducible graph snapshot."
        )

    typer.secho(
        f"OK: manifest v{m_version}, entry {server['entry_point']}, DOI {doi}",
        fg=typer.colors.GREEN,
    )
    return manifest


def _packer() -> list[str]:
    if shutil.which("mcpb"):
        return ["mcpb"]
    if shutil.which("npx"):
        return ["npx", "--yes", "@anthropic-ai/mcpb"]
    _fail("Neither 'mcpb' nor 'npx' found. Install with: npm i -g @anthropic-ai/mcpb")
    return []  # unreachable


def pack() -> None:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    cmd = [*_packer(), "pack", str(BUNDLE_ROOT), str(OUTPUT)]
    typer.echo("Running: " + " ".join(cmd))
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        _fail(f"mcpb pack failed (exit {result.returncode}).")
    size_mb = OUTPUT.stat().st_size / 1e6
    typer.secho(f"\nBuilt {OUTPUT} ({size_mb:.2f} MB)", fg=typer.colors.GREEN)


def main(
    check: bool = typer.Option(
        False, "--check", help="Validate the manifest and versions without packing."
    ),
) -> None:
    """Validate and pack the OptimusKG MCP bundle into dist/optimuskg.mcpb."""
    validate()
    if not check:
        pack()


if __name__ == "__main__":
    typer.run(main)
