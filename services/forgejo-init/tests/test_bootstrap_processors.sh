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
printf '%s\n' "$*" >> "$BOOTSTRAP_TEST_FORGEJO_LOG"
case "$*" in
  *"admin user list"*) printf '1 %s\n' "$FORGEJO_BOOTSTRAP_ADMIN_USERNAME" ;;
  *"admin user create"*) : > "$BOOTSTRAP_TEST_AGENT_USER_FILE" ;;
  *"actions generate-secret"*) printf '%s\n' test-runner-secret ;;
  *"admin user generate-access-token"*)
    case "$*" in
      *"--username $FORGEJO_GATEWAY_C37_118_AGENT_USERNAME"*) printf '%s\n' test-gateway-c37-118-agent-token ;;
      *) printf '%s\n' test-package-token ;;
    esac
    ;;
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
  cat > "$fake_bin/curl" <<'EOF'
#!/bin/sh
set -eu
output_file=/dev/null
method=GET
data=
url=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --output|-o) output_file="$2"; shift 2 ;;
    --request|-X) method="$2"; shift 2 ;;
    --data|--data-raw|--data-binary) data="$2"; shift 2 ;;
    --user|--header|-H) shift 2 ;;
    --fail|--silent|--show-error) shift ;;
    http://*|https://*) url="$1"; shift ;;
    *) shift ;;
  esac
done
printf '%s %s %s\n' "$method" "$url" "$data" >> "$BOOTSTRAP_TEST_CURL_LOG"
case "$url" in
  */api/v1/admin/users?limit=100)
    {
      printf '%s' "[{\"username\":\"$FORGEJO_BOOTSTRAP_ADMIN_USERNAME\",\"is_admin\":true,\"restricted\":false}"
      if [ -e "$BOOTSTRAP_TEST_AGENT_USER_FILE" ]; then
        printf '%s' ",{\"username\":\"$FORGEJO_GATEWAY_C37_118_AGENT_USERNAME\",\"is_admin\":false,\"restricted\":true}"
      fi
      printf '%s\n' ']'
    } > "$output_file"
    ;;
  */api/v1/user)
    printf '%s\n' "{\"username\":\"$FORGEJO_GATEWAY_C37_118_AGENT_USERNAME\"}" > "$output_file"
    ;;
  */collaborators/"$FORGEJO_GATEWAY_C37_118_AGENT_USERNAME")
    : > "$output_file"
    ;;
  */actions/workflows/gateway.yaml/dispatches)
    test "$method" = POST
    test "$data" = '{"ref":"main"}'
    ;;
  *) exit 1 ;;
esac
if [ "$output_file" != /dev/null ] && [ ! -s "$output_file" ] && [ "$method" != PUT ]; then
  printf '%s\n' "curl response was not written for $url" >&2
  exit 1
fi
EOF
  cat > "$fake_bin/jq" <<'EOF'
#!/bin/sh
set -eu
case "$*" in
  *'.[] | select('*)
    if [ ! -e "$BOOTSTRAP_TEST_AGENT_USER_FILE" ]; then
      exit 1
    fi
    printf '%s\n' "{\"username\":\"$FORGEJO_GATEWAY_C37_118_AGENT_USERNAME\",\"is_admin\":false,\"restricted\":true}"
    ;;
  *'.username == $username'*)
    [ -e "$BOOTSTRAP_TEST_AGENT_USER_FILE" ]
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
if [ "${1:-}" = "-C" ] && [ "${3:-}" = "init" ] && [ -e "$2/.git" ]; then
  printf '%s\n' "Bootstrap inherited .git metadata into its seed worktree" >&2
  exit 1
fi
case " $* " in
  *" ls-remote "*) [ -z "$BOOTSTRAP_TEST_REPOSITORY_REFS" ] || printf '%s\n' "$BOOTSTRAP_TEST_REPOSITORY_REFS" ;;
esac
EOF
  cat > "$fake_bin/chown" <<'EOF'
#!/bin/sh
exit 0
EOF
  chmod +x "$fake_bin/s6-setuidgid" "$fake_bin/forgejo" "$fake_bin/curl" "$fake_bin/jq" "$fake_bin/wget" "$fake_bin/git" "$fake_bin/chown"
}

setup_case() {
  case_directory="$temporary_root/$1"
  mkdir -p \
    "$case_directory/seeds/processor-frequency-scale" \
    "$case_directory/seeds/processor-apparent-power" \
    "$case_directory/seeds/processor-frequency-iec104-export" \
    "$case_directory/seeds/processor-frequency-measurement-session" \
    "$case_directory/seeds/processor-alarm-threshold" \
    "$case_directory/seeds/gateway-c37-118"
  printf '%s\n' frequency > "$case_directory/seeds/processor-frequency-scale/README.md"
  printf '%s\n' apparent > "$case_directory/seeds/processor-apparent-power/README.md"
  printf '%s\n' frequency-iec104-export > "$case_directory/seeds/processor-frequency-iec104-export/README.md"
  printf '%s\n' frequency-measurement-session > "$case_directory/seeds/processor-frequency-measurement-session/README.md"
  printf '%s\n' alarm-threshold > "$case_directory/seeds/processor-alarm-threshold/README.md"
  printf '%s\n' gateway-c37-118 > "$case_directory/seeds/gateway-c37-118/README.md"
  mkdir "$case_directory/seeds/gateway-c37-118/.git"
  printf '%s\n' copied-git-metadata > "$case_directory/seeds/gateway-c37-118/.git/config"
  : > "$case_directory/app.ini"
  : > "$case_directory/forgejo.log"
  : > "$case_directory/git.log"
  : > "$case_directory/curl.log"
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
    FORGEJO_GATEWAY_C37_118_AGENT_USERNAME=wama-gateway-c37-118-agent \
    FORGEJO_GATEWAY_C37_118_AGENT_EMAIL=wama-gateway-c37-118-agent@test \
    FORGEJO_RUNNER_URL=http://forgejo.test/ \
    WAMA_FREQUENCY_SCALE_DEPLOY_ROOT="$case_directory/frequency-deploy" \
    WAMA_APPARENT_POWER_DEPLOY_ROOT="$case_directory/apparent-deploy" \
    WAMA_FREQUENCY_IEC104_EXPORT_DEPLOY_ROOT="$case_directory/frequency-iec104-export-deploy" \
    WAMA_FREQUENCY_MEASUREMENT_SESSION_DEPLOY_ROOT="$case_directory/frequency-measurement-session-deploy" \
    WAMA_ALARM_THRESHOLD_DEPLOY_ROOT="$case_directory/alarm-threshold-deploy" \
    WAMA_GATEWAY_C37_118_DEPLOY_ROOT="$case_directory/gateway-c37-118-deploy" \
    BOOTSTRAP_TEST_AGENT_USER_FILE="$case_directory/gateway-c37-118-agent-user" \
    BOOTSTRAP_TEST_CURL_LOG="$case_directory/curl.log" \
    BOOTSTRAP_TEST_FORGEJO_LOG="$case_directory/forgejo.log" \
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

assert_not_contains() {
  unexpected="$1"
  path="$2"
  if grep -Fq "$unexpected" "$path"; then
    printf '%s\n' "Expected $path not to contain $unexpected" >&2
    exit 1
  fi
}

test_seeds_and_registers_all_processor_repositories() {
  setup_case seed-new
  export BOOTSTRAP_TEST_REPOSITORY_EXISTS=false
  export BOOTSTRAP_TEST_REPOSITORY_REFS=
  run_bootstrap > "$case_directory/bootstrap.log" 2>&1
  assert_contains processor-frequency-scale.git "$case_directory/git.log"
  assert_contains processor-apparent-power.git "$case_directory/git.log"
  assert_contains processor-frequency-iec104-export.git "$case_directory/git.log"
  assert_contains processor-frequency-measurement-session.git "$case_directory/git.log"
  assert_contains processor-alarm-threshold.git "$case_directory/git.log"
  assert_contains gateway-c37-118.git "$case_directory/git.log"
  assert_contains wama-processor-frequency-scale-ci: "$case_directory/runner/config.yaml"
  assert_contains wama-processor-frequency-scale-deploy: "$case_directory/runner/config.yaml"
  assert_contains wama-processor-apparent-power-ci: "$case_directory/runner/config.yaml"
  assert_contains wama-processor-apparent-power-deploy: "$case_directory/runner/config.yaml"
  assert_contains wama-processor-frequency-iec104-export-ci: "$case_directory/runner/config.yaml"
  assert_contains wama-processor-frequency-iec104-export-deploy: "$case_directory/runner/config.yaml"
  assert_contains wama-processor-frequency-measurement-session-ci: "$case_directory/runner/config.yaml"
  assert_contains wama-processor-frequency-measurement-session-deploy: "$case_directory/runner/config.yaml"
  assert_contains wama-processor-alarm-threshold-ci: "$case_directory/runner/config.yaml"
  assert_contains wama-processor-alarm-threshold-deploy: "$case_directory/runner/config.yaml"
  assert_contains wama-gateway-c37-118-ci: "$case_directory/runner/config.yaml"
  assert_contains wama-gateway-c37-118-deploy: "$case_directory/runner/config.yaml"
  assert_contains twelve-connections-v6 "$case_directory/runner/forgejo-managed-repositories.layout"
  assert_contains 'wama-admin/processor-frequency-measurement-session' "$case_directory/runner/forgejo-managed-repositories.scope"
  assert_contains 'wama-admin/processor-alarm-threshold' "$case_directory/runner/forgejo-managed-repositories.scope"
  assert_contains "$case_directory/frequency-measurement-session-deploy" "$case_directory/runner/config.yaml"
  assert_contains "$case_directory/alarm-threshold-deploy" "$case_directory/runner/config.yaml"
  assert_contains 'PUT http://forgejo.test/api/v1/repos/wama-admin/gateway-c37-118/collaborators/wama-gateway-c37-118-agent {"permission":"write"}' "$case_directory/curl.log"
  assert_not_contains '/actions/workflows/' "$case_directory/curl.log"
  test -f "$case_directory/runner/gateway-c37-118.workflow-triggered"
  assert_contains 'owner=wama-admin' "$case_directory/runner/forgejo-gateway-c37-118-agent.identity"
  assert_contains 'repository=gateway-c37-118' "$case_directory/runner/forgejo-gateway-c37-118-agent.identity"
  assert_contains 'username=wama-gateway-c37-118-agent' "$case_directory/runner/forgejo-gateway-c37-118-agent.identity"
  test "$(stat -c '%a' "$case_directory/runner/forgejo-gateway-c37-118-agent.token")" = 600
  if grep -Eq 'test-(gateway-c37-118-agent-token|package-token)' "$case_directory/bootstrap.log"; then
    printf '%s\n' "Bootstrap wrote a Forgejo token to its log" >&2
    exit 1
  fi
  export BOOTSTRAP_TEST_REPOSITORY_REFS="deadbeef refs/heads/main"
  run_bootstrap >> "$case_directory/bootstrap.log" 2>&1
  test "$(grep -Fc -- "admin user create --username wama-gateway-c37-118-agent" "$case_directory/forgejo.log")" -eq 1
  assert_not_contains '/actions/workflows/' "$case_directory/curl.log"
  test -f "$case_directory/frequency-deploy/.wama-forgejo-processor-root"
  test -f "$case_directory/apparent-deploy/.wama-forgejo-processor-root"
  test -f "$case_directory/frequency-iec104-export-deploy/.wama-forgejo-processor-root"
  test -f "$case_directory/frequency-measurement-session-deploy/.wama-forgejo-processor-root"
  test -f "$case_directory/alarm-threshold-deploy/.wama-forgejo-processor-root"
  test -f "$case_directory/gateway-c37-118-deploy/.wama-forgejo-gateway-c37-118-root"
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
  assert_contains "processor-frequency-iec104-export already has refs; leaving it unchanged" "$case_directory/bootstrap.log"
  assert_contains "processor-frequency-measurement-session already has refs; leaving it unchanged" "$case_directory/bootstrap.log"
  assert_contains "gateway-c37-118 already has refs; leaving it unchanged" "$case_directory/bootstrap.log"
  assert_contains 'POST http://forgejo.test/api/v1/repos/wama-admin/gateway-c37-118/actions/workflows/gateway.yaml/dispatches {"ref":"main"}' "$case_directory/curl.log"
  test "$(grep -Fc '/actions/workflows/' "$case_directory/curl.log")" -eq 1
  test -f "$case_directory/runner/gateway-c37-118.workflow-triggered"
  run_bootstrap >> "$case_directory/bootstrap.log" 2>&1
  test "$(grep -Fc '/actions/workflows/' "$case_directory/curl.log")" -eq 1
}

test_preserves_gateway_workflow_marker_across_runner_scope_reset() {
  setup_case preserve-gateway-workflow-marker
  mkdir -p "$case_directory/runner"
  printf '%s\n' twelve-connections-v6 > "$case_directory/runner/forgejo-managed-repositories.layout"
  printf '%s\n' stale-scope > "$case_directory/runner/forgejo-managed-repositories.scope"
  : > "$case_directory/runner/gateway-c37-118.workflow-triggered"
  export BOOTSTRAP_TEST_REPOSITORY_EXISTS=true
  export BOOTSTRAP_TEST_REPOSITORY_REFS="deadbeef refs/heads/main"
  run_bootstrap > "$case_directory/bootstrap.log" 2>&1
  assert_not_contains '/actions/workflows/' "$case_directory/curl.log"
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

test_rejects_unmarked_nonempty_frequency_iec104_export_root() {
  setup_case reject-iec104-root
  mkdir "$case_directory/frequency-iec104-export-deploy"
  printf '%s\n' unmanaged > "$case_directory/frequency-iec104-export-deploy/file"
  export BOOTSTRAP_TEST_REPOSITORY_EXISTS=true
  export BOOTSTRAP_TEST_REPOSITORY_REFS="deadbeef refs/heads/main"
  if run_bootstrap > "$case_directory/bootstrap.log" 2>&1; then
    printf '%s\n' "Bootstrap accepted an unmarked nonempty IEC 104 processor root" >&2
    exit 1
  fi
  assert_contains "must be empty before bootstrap creates its marker" "$case_directory/bootstrap.log"
}

test_rejects_unmarked_nonempty_frequency_measurement_session_root() {
  setup_case reject-measurement-session-root
  mkdir "$case_directory/frequency-measurement-session-deploy"
  printf '%s\n' unmanaged > "$case_directory/frequency-measurement-session-deploy/file"
  export BOOTSTRAP_TEST_REPOSITORY_EXISTS=true
  export BOOTSTRAP_TEST_REPOSITORY_REFS="deadbeef refs/heads/main"
  if run_bootstrap > "$case_directory/bootstrap.log" 2>&1; then
    printf '%s\n' "Bootstrap accepted an unmarked nonempty measurement-session processor root" >&2
    exit 1
  fi
  assert_contains "must be empty before bootstrap creates its marker" "$case_directory/bootstrap.log"
}

test_rejects_unmarked_nonempty_alarm_threshold_root() {
  setup_case reject-alarm-threshold-root
  mkdir "$case_directory/alarm-threshold-deploy"
  printf '%s\n' unmanaged > "$case_directory/alarm-threshold-deploy/file"
  export BOOTSTRAP_TEST_REPOSITORY_EXISTS=true
  export BOOTSTRAP_TEST_REPOSITORY_REFS="deadbeef refs/heads/main"
  if run_bootstrap > "$case_directory/bootstrap.log" 2>&1; then
    printf '%s\n' "Bootstrap accepted an unmarked nonempty alarm-threshold processor root" >&2
    exit 1
  fi
  assert_contains "must be empty before bootstrap creates its marker" "$case_directory/bootstrap.log"
}

test_rejects_unmarked_nonempty_gateway_c37_118_root() {
  setup_case reject-gateway-root
  mkdir "$case_directory/gateway-c37-118-deploy"
  printf '%s\n' unmanaged > "$case_directory/gateway-c37-118-deploy/file"
  export BOOTSTRAP_TEST_REPOSITORY_EXISTS=true
  export BOOTSTRAP_TEST_REPOSITORY_REFS="deadbeef refs/heads/main"
  if run_bootstrap > "$case_directory/bootstrap.log" 2>&1; then
    printf '%s\n' "Bootstrap accepted an unmarked nonempty gateway-c37-118 root" >&2
    exit 1
  fi
  assert_contains "must be empty before bootstrap creates its marker" "$case_directory/bootstrap.log"
}

test_seeds_and_registers_all_processor_repositories
test_skips_nonempty_repositories
test_preserves_gateway_workflow_marker_across_runner_scope_reset
test_rejects_unmarked_nonempty_processor_root
test_rejects_unmarked_nonempty_frequency_iec104_export_root
test_rejects_unmarked_nonempty_frequency_measurement_session_root
test_rejects_unmarked_nonempty_alarm_threshold_root
test_rejects_unmarked_nonempty_gateway_c37_118_root