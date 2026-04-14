"""Relative-path <-> Dataverse (directoryLabel, filename) conversion."""

from __future__ import annotations


def to_relative(directory_label: str | None, filename: str) -> str:
    """Join a Dataverse ``directoryLabel`` and ``filename`` into a POSIX relative path."""
    if not directory_label:
        return filename
    return f"{directory_label.strip('/')}/{filename}"


def split_relative(relative_path: str) -> tuple[str, str]:
    """Split a relative path into ``(directory_label, filename)``.

    An empty ``directory_label`` means the file sits at the dataset root.
    """
    cleaned = relative_path.strip("/")
    if "/" not in cleaned:
        return "", cleaned
    directory, filename = cleaned.rsplit("/", 1)
    return directory, filename
