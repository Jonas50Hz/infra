#!/bin/sh

set -eu

repository_root="$(cd "$(dirname "$0")/../../.." && pwd)"
bootstrap_script="$repository_root/services/forgejo-init/bootstrap-processors.sh"
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
  *"admin user list"*) printf '1 %s\n' "$FORGEJO_BOOTSTRAP_ADMIN_USERNAME" ;;
  *"actions generate-secret"*) printf '%s\n' test-runner-secret ;;
  *"admin user generate-access-token"*) printf '%s\n' test-package-token ;;
  *"actions register"*)
    while [ "$#" -gt 0 ]; do
      if [ "$1" = "--name" ]; then
        printf '%s-uuid\n' "$2"
        exit 0
      fi
      shift
    done
    exit 1
    ;;
  *) exit 1 ;;
esac
EOF
  cat > "$fake_bin/wget" <<'EOF'
#!/bin/sh
set -eu
output_file=/dev/null
method=GET
is_post=false
url=
while [ "$#" -gt 0 ]; do
  case "$1" in
    -O) output_file="$2"; shift 2 ;;
    --method=*) method="${1#--method=}"; shift ;;
    --post-data=*) is_post=true; shift ;;
    --post-data) is_post=true; shift 2 ;;
    http://*|https://*) url="$1"; shift ;;
    *) shift ;;
  esac
done
printf '%s %s\n' "$method" "$url" >> "$BOOTSTRAP_TEST_WGET_LOG"
if [ "$method" = DELETE ]; then
  exit 0
fi
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
  *" ls-remote "*) [ -z "$BOOTSTRAP_TEST_REPOSITORY_REFS" ] || printf '%s\n' "$BOOTSTRAP_TEST_REPOSITORY_REFS" ;;
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
  mkdir -p "$case_directory/seeds/processor-frequency-scale" "$case_directory/seeds/processor-apparent-power"
  printf '%s\n' frequency > "$case_directory/seeds/processor-frequency-scale/README.md"
  printf '%s\n' apparent > "$case_directory/seeds/processor-apparent-power/README.md"
  : > "$case_directory/app.ini"
  : > "$case_directory/git.log"
  : > "$case_directory/wget.log"
  create_fake_commands "$case_directory/bin"
}

run_bootstrap() {
  PATH="$case_directory/bin:$PATH" \
    FORGEJO_SERVER_CONFIG="$case_directory/app.ini" \
    FORGEJO_RUNNER_DIRECTORY="$case_directory/runner" \
    FORGEJO_PROCESSOR_SEED_ROOT="$case_directory/seeds" \
    FORGEJO_API_URL=http://forgejo.test/api/v1 \
    FORGEJO_BOOTSTRAP_ADMIN_USERNAME=wama-admin \
    FORGEJO_BOOTSTRAP_ADMIN_PASSWORD=wama-admin \
    FORGEJO_RUNNER_URL=http://forgejo.test/ \
    WAMA_FREQUENCY_SCALE_DEPLOY_ROOT="$case_directory/frequency-deploy" \
    WAMA_APPARENT_POWER_DEPLOY_ROOT="$case_directory/apparent-deploy" \
    BOOTSTRAP_TEST_GIT_LOG="$case_directory/git.log" \
    BOOTSTRAP_TEST_WGET_LOG="$case_directory/wget.log" \
    sh "$bootstrap_script"
}

assert_contains() {
  expected="$1"
  path="$2"
  grep -Fq "$expected" "$path" || {
    printf '%s\n' "Expected $path to contain $expected" >&2
    exit 1
  }
}

test_seeds_and_registers_all_processor_repositories() {
  setup_case seed-new
  export BOOTSTRAP_TEST_REPOSITORY_EXISTS=false
  export BOOTSTRAP_TEST_REPOSITORY_REFS=
  run_bootstrap > "$case_directory/bootstrap.log" 2>&1
  assert_contains processor-frequency-scale.git "$case_directory/git.log"
  assert_contains processor-apparent-power.git "$case_directory/git.log"
  assert_contains wama-processor-frequency-scale-ci: "$case_directory/runner/config.yaml"
  assert_contains wama-processor-frequency-scale-deploy: "$case_directory/runner/config.yaml"
  assert_contains wama-processor-apparent-power-ci: "$case_directory/runner/config.yaml"
  assert_contains wama-processor-apparent-power-deploy: "$case_directory/runner/config.yaml"
  assert_contains four-connections-v2 "$case_directory/runner/forgejo-processor-repositories.layout"
  test -f "$case_directory/frequency-deploy/.wama-forgejo-processor-root"
  test -f "$case_directory/apparent-deploy/.wama-forgejo-processor-root"
}

test_skips_nonempty_repositories() {
  setup_case skip-nonempty
  export BOOTSTRAP_TEST_REPOSITORY_EXISTS=true
  export BOOTSTRAP_TEST_REPOSITORY_REFS="deadbeef refs/heads/main"
  run_bootstrap > "$case_directory/bootstrap.log" 2>&1
  if grep -Eq '(^| )(init|commit|push)( |$)' "$case_directory/git.log"; then
    printf '%s\n' "Bootstrap changed a repository with refs" >&2
    exit 1
  fi
  assert_contains "processor-frequency-scale already has refs; leaving it unchanged" "$case_directory/bootstrap.log"
  assert_contains "processor-apparent-power already has refs; leaving it unchanged" "$case_directory/bootstrap.log"
}

test_rejects_unmarked_nonempty_processor_root() {
  setup_case reject-root
  mkdir "$case_directory/frequency-deploy"
  printf '%s\n' unmanaged > "$case_directory/frequency-deploy/file"
  export BOOTSTRAP_TEST_REPOSITORY_EXISTS=true
  export BOOTSTRAP_TEST_REPOSITORY_REFS="deadbeef refs/heads/main"
  if run_bootstrap > "$case_directory/bootstrap.log" 2>&1; then
    printf '%s\n' "Bootstrap accepted an unmarked nonempty processor root" >&2
    exit 1
  fi
  assert_contains "must be empty before bootstrap creates its marker" "$case_directory/bootstrap.log"
}

test_seeds_and_registers_all_processor_repositories
test_skips_nonempty_repositories
test_rejects_unmarked_nonempty_processor_root