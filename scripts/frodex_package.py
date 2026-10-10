#!/usr/bin/env python3
"""Validate Frodex release archives before publishing or executing their binaries."""

import argparse
import hashlib
import json
import shutil
import tarfile
from pathlib import Path, PurePosixPath


TARGETS = {
    "aarch64-apple-darwin",
    "x86_64-apple-darwin",
    "aarch64-unknown-linux-gnu",
    "x86_64-unknown-linux-gnu",
}
PAIR = ("codex", "codex-code-mode-host")
PACKAGE_DIRS = {
    "bin",
    "codex-path",
    "codex-resources",
    "codex-resources/zsh",
    "codex-resources/zsh/bin",
}


def expected_files(target: str) -> set[str]:
    """Files emitted by the upstream builder for each supported release target."""
    if target not in TARGETS:
        raise ValueError(f"unsupported Frodex package target: {target}")
    files = {
        "codex-package.json",
        "bin/codex",
        "bin/codex-code-mode-host",
        "codex-path/rg",
        "codex-resources/zsh/bin/zsh",
    }
    if target.endswith("-linux-gnu"):
        files.add("codex-resources/bwrap")
    return files


def _unique_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate package metadata key: {key}")
        result[key] = value
    return result


def _members(archive: tarfile.TarFile) -> dict[str, tarfile.TarInfo]:
    members = {}
    for member in archive.getmembers():
        name = member.name
        if (
            not name
            or "\\" in name
            or name != PurePosixPath(name).as_posix()
            or PurePosixPath(name).is_absolute()
            or ".." in PurePosixPath(name).parts
        ):
            raise ValueError(f"noncanonical archive member: {name!r}")
        if name in members:
            raise ValueError(f"duplicate archive member: {name}")
        if not (member.isfile() or member.isdir()) or member.issparse():
            raise ValueError(
                f"archive member must be a regular file or directory: {name}"
            )
        if member.mode & 0o7000:
            raise ValueError(f"privileged archive member permissions: {name}")
        if not member.mode & 0o400 or (member.isdir() and not member.mode & 0o100):
            raise ValueError(f"unreadable archive member: {name}")
        members[name] = member
    return members


def _validate_members(
    archive: tarfile.TarFile,
    members: dict[str, tarfile.TarInfo],
    expected_version: str,
    expected_target: str,
    allow_legacy: bool,
) -> tuple[str, dict, dict[str, str]]:
    required = expected_files(expected_target)
    files = {name for name, member in members.items() if member.isfile()}
    dirs = set(members) - files
    metadata = {}
    if allow_legacy and files == set(PAIR) and not dirs:
        # Old admitted releases are immutable and retain their flat layout.
        layout = "legacy-flat"
        pair_paths = {name: name for name in PAIR}
    else:
        layout = "upstream-v1"
        pair_paths = {name: f"bin/{name}" for name in PAIR}
        if files != required or not dirs <= PACKAGE_DIRS:
            raise ValueError(
                f"invalid package members: missing={sorted(required - files)}, "
                f"unexpected={sorted((files - required) | (dirs - PACKAGE_DIRS))}"
            )
        manifest = members["codex-package.json"]
        if not 0 < manifest.size <= 16384:
            raise ValueError("invalid package metadata size")
        with archive.extractfile(manifest) as stream:
            metadata = json.loads(
                stream.read().decode("utf-8"), object_pairs_hook=_unique_object
            )
        expected_metadata = {
            "layoutVersion": 1,
            "version": expected_version,
            "target": expected_target,
            "variant": "codex",
            "entrypoint": "bin/codex",
            "resourcesDir": "codex-resources",
            "pathDir": "codex-path",
        }
        if (
            metadata != expected_metadata
            or type(metadata.get("layoutVersion")) is not int
        ):
            raise ValueError(
                f"invalid package metadata: expected {expected_metadata!r}"
            )
    for name in files:
        member = members[name]
        if member.size <= 0:
            raise ValueError(f"empty package file: {name}")
        if name != "codex-package.json" and not member.mode & 0o100:
            raise ValueError(f"package file is not owner-executable: {name}")
    return layout, metadata, pair_paths


def _inspect_archive(
    archive: tarfile.TarFile,
    expected_version: str,
    expected_target: str,
    allow_legacy: bool,
    destination: Path | None,
) -> dict:
    members = _members(archive)
    layout, metadata, pair_paths = _validate_members(
        archive, members, expected_version, expected_target, allow_legacy
    )
    files = {}
    for name, member in members.items():
        if member.isdir():
            if destination is not None:
                (destination / name).mkdir(parents=True, exist_ok=True)
            continue
        output = None
        if destination is not None:
            path = destination / name
            path.parent.mkdir(parents=True, exist_ok=True)
            output = path.open("xb")
        digest = hashlib.sha256()
        try:
            with archive.extractfile(member) as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(chunk)
                    if output is not None:
                        output.write(chunk)
        finally:
            if output is not None:
                output.close()
        if destination is not None:
            (destination / name).chmod(0o644 if name == "codex-package.json" else 0o755)
        files[name] = {
            "sha256": digest.hexdigest(),
            "size": member.size,
            "mode": member.mode,
        }
    return {
        "layout": layout,
        "metadata": metadata,
        "members": sorted(members),
        "files": files,
        "executables": {
            name: {
                "path": path,
                "sha256": files[path]["sha256"],
                "size": files[path]["size"],
            }
            for name, path in pair_paths.items()
        },
    }


def validate_archive(
    path: Path | str,
    expected_version: str,
    expected_target: str,
    *,
    allow_legacy: bool = False,
) -> dict:
    """Return member hashes and metadata; legacy promotion requires opt-in."""
    with tarfile.open(path, "r:gz") as archive:
        return _inspect_archive(
            archive, expected_version, expected_target, allow_legacy, None
        )


def extract_archive(
    path: Path | str,
    destination: Path | str,
    expected_version: str,
    expected_target: str,
    *,
    allow_legacy: bool = False,
) -> dict:
    """Extract validated regular files into a new directory without following archive links."""
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=False)
    try:
        with tarfile.open(path, "r:gz") as archive:
            return _inspect_archive(
                archive, expected_version, expected_target, allow_legacy, destination
            )
    except BaseException:
        # Never leave a partially validated package available for execution.
        shutil.rmtree(destination)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("--version", required=True)
    parser.add_argument("--target", required=True, choices=sorted(TARGETS))
    parser.add_argument("--allow-legacy", action="store_true")
    parser.add_argument("--extract", type=Path)
    args = parser.parse_args()
    kwargs = dict(
        expected_version=args.version,
        expected_target=args.target,
        allow_legacy=args.allow_legacy,
    )
    if args.extract is None:
        result = validate_archive(args.archive, **kwargs)
    else:
        result = extract_archive(args.archive, args.extract, **kwargs)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
