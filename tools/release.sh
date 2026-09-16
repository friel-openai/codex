#!/usr/bin/env bash

set -euo pipefail

usage() {
	cat <<'EOF'
Usage: tools/release.sh [-b BODY]

Dispatch the default-branch Frodex release workflow. The workflow
materializes the configured Codex base, chooses the next version, publishes
the projection tag, builds release binaries, and creates the GitHub release.

Options:
  -b BODY     Include BODY in the annotated tag and GitHub release notes.
  -h, --help  Show this help.
EOF
}

die() {
	echo "release.sh: $*" >&2
	exit 1
}

body=
while (($# > 0)); do
	case "$1" in
	-b)
		(($# >= 2)) || die "-b requires a value"
		[[ -n "$2" ]] || die "-b requires a non-empty body"
		body=$2
		shift 2
		;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		die "unknown argument: $1"
		;;
	esac
done

repository=$(git rev-parse --show-toplevel 2>/dev/null) ||
	die "run this command from the Frodex repository"
cd "$repository"

command -v gh >/dev/null || die "gh is required to dispatch the release workflow"

arguments=(workflow run frodex-release.yml)
if [[ -n "$body" ]]; then
	arguments+=(-f "release_notes=$body")
fi
gh "${arguments[@]}"
