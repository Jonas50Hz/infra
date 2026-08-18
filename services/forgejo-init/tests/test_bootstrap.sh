#!/bin/sh

set -eu

repository_root="$(cd "$(dirname "$0")/../../.." && pwd)"
bootstrap_script="$repository_root/services/forgejo-init/bootstrap.sh"
temporary_root="$(mktemp -d)"

cleanup() {
  rm -rf "$temporary_root"
}

trap cleanup EXIT HUP INT TERM

create_fake_commands() {
  fake_bin="$1"
  mkdir "$fake_bin"

  cat > "$fake_bin/s6-setuidgid" <<'EOF'
#!/bin/sh
shift
exec "$@"
EOF

  cat > "$fake_bin/forgejo" <<'EOF'
#!/bin/sh
set -eu

case "$*" in
  *"admin user list"*)
    printf '1 %s\n' "$FORGEJO_BOOTSTRAP_ADMIN_USERNAME"
    ;;
  *"actions generate-secret"*)
    printf '%s\n' "test-runner-secret"
    ;;
  *"admin user generate-access-token"*)
    printf '%s\n' "test-package-token"
    ;;
  *"actions register"*)
    runner_name=
    while [ "$#" -gt 0 ]; do
      if [ "$1" = "--name" ]; then
        runner_name="$2"
        break
      fi
      shift
    done
    [ -n "$runner_name" ]
    printf '%s-uuid\n' "$runner_name"
    ;;
  *)
    printf '%s\n' "Unexpected Forgejo command: $*" >&2
    exit 1
    ;;
esac
EOF

  cat > "$fake_bin/wget" <<'EOF'
#!/bin/sh
set -eu

output_file=/dev/null
is_post=false
while [ "$#" -gt 0 ]; do
  case "$1" in
    -O)
      output_file="$2"
      shift 2
      ;;
    --post-data=*)
      is_post=true
      shift
      ;;
    --post-data)
      is_post=true
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done

if [ "$is_post" = false ] && [ "$BOOTSTRAP_TEST_REPOSITORY_EXISTS" = false ]; then
  exit 8
fi
printf '%s\n' '{"private":true}' > "$output_file"
EOF

  cat > "$fake_bin/git" <<'EOF'
#!/bin/sh
set -eu

printf '%s\n' "$*" >> "$BOOTSTRAP_TEST_GIT_LOG"
case " $* " in
  *" ls-remote "*)
    if [ -n "$BOOTSTRAP_TEST_REPOSITORY_REFS" ]; then
      printf '%s\n' "$BOOTSTRAP_TEST_REPOSITORY_REFS"
    fi
    ;;
esac
EOF

  cat > "$fake_bin/chown" <<'EOF'
#!/bin/sh
exit 0
EOF

  chmod +x "$fake_bin/s6-setuidgid" "$fake_bin/forgejo" "$fake_bin/wget" "$fake_bin/git" "$fake_bin/chown"
}

setup_case() {
  case_directory="$temporary_root/$1"
  mkdir -p "$case_directory/seed"
  printf '%s\n' "seed" > "$case_directory/seed/README.md"
  : > "$case_directory/app.ini"
  : > "$case_directory/git.log"
  create_fake_commands "$case_directory/bin"
}

run_bootstrap() {
  PATH="$case_directory/bin:$PATH" \
    FORGEJO_SERVER_CONFIG="$case_directory/app.ini" \
    FORGEJO_RUNNER_DIRECTORY="$case_directory/runner" \
    FORGEJO_PROCESSORS_SEED_DIRECTORY="$case_directory/seed" \
    FORGEJO_API_URL="http://forgejo.test/api/v1" \
    FORGEJO_BOOTSTRAP_ADMIN_USERNAME="wama-admin" \
    FORGEJO_BOOTSTRAP_ADMIN_PASSWORD="wama-admin" \
    FORGEJO_RUNNER_URL="http://forgejo.test/" \
    WAMA_PROCESSORS_DEPLOY_ROOT="$case_directory/deploy" \
    BOOTSTRAP_TEST_GIT_LOG="$case_directory/git.log" \
    sh "$bootstrap_script"
}

assert_contains() {
  expected="$1"
  path="$2"
  if ! grep -Fq "$expected" "$path"; then
    printf '%s\n' "Expected $path to contain $expected" >&2
    exit 1
  fi
}

test_seeds_new_repository() {
  setup_case seed-new
  export BOOTSTRAP_TEST_REPOSITORY_EXISTS=false
  export BOOTSTRAP_TEST_REPOSITORY_REFS=

  run_bootstrap > "$case_directory/bootstrap.log" 2>&1

  assert_contains " push " "$case_directory/git.log"
  if grep -Eq '(^| )(--force|reset)( |$)' "$case_directory/git.log"; then
    printf '%s\n' "Bootstrap seed used a destructive Git command" >&2
    exit 1
  fi
  assert_contains "wama-processors-ci:" "$case_directory/runner/config.yaml"
  assert_contains "wama-processors-deploy:" "$case_directory/runner/config.yaml"
  assert_contains 'FORGEJO_PROCESSORS_PACKAGE_USERNAME: "wama-admin"' "$case_directory/runner/config.yaml"
  assert_contains 'FORGEJO_PROCESSORS_PACKAGE_TOKEN: "test-package-token"' "$case_directory/runner/config.yaml"
  assert_contains "two-connections-v1" "$case_directory/runner/forgejo-processors.layout"
  test -f "$case_directory/runner/forgejo-processors-package.token"
  test -f "$case_directory/deploy/.wama-forgejo-processors-root"
}

test_skips_nonempty_repository() {
  setup_case skip-nonempty
  export BOOTSTRAP_TEST_REPOSITORY_EXISTS=true
  export BOOTSTRAP_TEST_REPOSITORY_REFS="deadbeef refs/heads/main"

  run_bootstrap > "$case_directory/bootstrap.log" 2>&1

  if grep -Eq '(^| )(init|commit|push)( |$)' "$case_directory/git.log"; then
    printf '%s\n' "Bootstrap must not change a repository with refs" >&2
    exit 1
  fi
  if grep -Eq '(^| )(--force|reset)( |$)' "$case_directory/git.log"; then
    printf '%s\n' "Bootstrap used a destructive Git command" >&2
    exit 1
  fi
  assert_contains "already has refs; leaving it unchanged" "$case_directory/bootstrap.log"
  assert_contains "wama-processors-ci:" "$case_directory/runner/config.yaml"
  assert_contains "wama-processors-deploy:" "$case_directory/runner/config.yaml"
}

test_replaces_legacy_runner_state() {
  setup_case replace-legacy-runner-state
  mkdir -p "$case_directory/runner"
  printf '%s\n' "legacy" > "$case_directory/runner/forgejo-runner.secret"
  printf '%s\n' "legacy" > "$case_directory/runner/.runner"
  export BOOTSTRAP_TEST_REPOSITORY_EXISTS=true
  export BOOTSTRAP_TEST_REPOSITORY_REFS="deadbeef refs/heads/main"

  run_bootstrap > "$case_directory/bootstrap.log" 2>&1

  test ! -e "$case_directory/runner/.runner"
  test ! -e "$case_directory/runner/forgejo-runner.secret"
  assert_contains "two-connections-v1" "$case_directory/runner/forgejo-processors.layout"
}

test_rejects_unmarked_nonempty_deployment_root() {
  setup_case reject-unmarked-root
  mkdir "$case_directory/deploy"
  printf '%s\n' "unmanaged" > "$case_directory/deploy/unmanaged.txt"
  export BOOTSTRAP_TEST_REPOSITORY_EXISTS=true
  export BOOTSTRAP_TEST_REPOSITORY_REFS="deadbeef refs/heads/main"

  if run_bootstrap > "$case_directory/bootstrap.log" 2>&1; then
    printf '%s\n' "Bootstrap accepted an unmarked nonempty deployment root" >&2
    exit 1
  fi
  assert_contains "must be empty before bootstrap creates its marker" "$case_directory/bootstrap.log"
  test ! -e "$case_directory/deploy/.wama-forgejo-processors-root"
}

test_seeds_new_repository
test_skips_nonempty_repository
test_replaces_legacy_runner_state
test_rejects_unmarked_nonempty_deployment_root