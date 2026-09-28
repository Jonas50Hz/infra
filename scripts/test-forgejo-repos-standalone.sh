#!/bin/sh
# Prove every Forgejo repository builds and tests using only its own directory.
#
# Each directory under forgejo-repos/ is an independent repository boundary. It
# must never read the parent checkout at build time. This script enforces that
# by building each repository with its own directory as the only build context,
# which makes any escape to ../ a hard build failure rather than a latent one.

set -eu

repository_root="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
seed_root="$repository_root/forgejo-repos"

failures=0
checked=0

for candidate in "$seed_root"/*/; do
	repository="${candidate%/}"
	name="$(basename "$repository")"

	# history/ holds archive bundles, not a repository.
	if [ ! -f "$repository/Dockerfile" ]; then
		continue
	fi

	# A repository that reaches outside its own directory is not standalone,
	# regardless of whether the build happens to succeed from the repo root.
	if grep -nE '^[[:space:]]*(COPY|ADD)[[:space:]]' "$repository/Dockerfile" \
		| grep -qE '\.\./|[[:space:]]/?docs/'; then
		printf '%-42s ESCAPES ITS OWN DIRECTORY\n' "$name"
		grep -nE '^[[:space:]]*(COPY|ADD)[[:space:]]' "$repository/Dockerfile" \
			| grep -E '\.\./|[[:space:]]/?docs/' >&2
		failures=$((failures + 1))
		continue
	fi

	checked=$((checked + 1))
	if docker build --target test --file "$repository/Dockerfile" "$repository" >/dev/null 2>&1; then
		printf '%-42s standalone build OK\n' "$name"
	else
		printf '%-42s STANDALONE BUILD FAILED\n' "$name"
		docker build --target test --file "$repository/Dockerfile" "$repository" 2>&1 | tail -20 >&2
		failures=$((failures + 1))
	fi
done

if [ "$failures" -ne 0 ]; then
	printf '\n%s repository/repositories are not standalone.\n' "$failures" >&2
	exit 1
fi

printf '\nAll %s Forgejo repositories build and test standalone.\n' "$checked"
