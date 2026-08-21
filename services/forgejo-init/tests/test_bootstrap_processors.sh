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
      *"--username $FORGEJO_GATEWAY_C37_118_ONBOARDING_AGENT_USERNAME"*) printf '%s\n' test-gateway-onboarding-agent-token ;;
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
        printf '%s' ",{\"username\":\"$FORGEJO_GATEWAY_C37_118_ONBOARDING_AGENT_USERNAME\",\"is_admin\":false,\"restricted\":true}"
      fi
      printf '%s\n' ']'
    } > "$output_file"
    ;;
  */api/v1/user)
    printf '%s\n' "{\"username\":\"$FORGEJO_GATEWAY_C37_118_ONBOARDING_AGENT_USERNAME\"}" > "$output_file"
    ;;
  */collaborators/"$FORGEJO_GATEWAY_C37_118_ONBOARDING_AGENT_USERNAME")
    : > "$output_file"
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
    printf '%s\n' "{\"username\":\"$FORGEJO_GATEWAY_C37_118_ONBOARDING_AGENT_USERNAME\",\"is_admin\":false,\"restricted\":true}"
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
  chmod +x "$fake_bin/s6-setuidgid" "$fake_bin/forgejo" "$fake_bin/curl" "$fake_bin/jq" "$fake_bin/wget" "$fake_bin/git" "$fake_bin/chown"
}

create_registry_stub() {
  path="$1"
  cat > "$path" <<'EOF'
import os
from pathlib import Path
import sys

if sys.argv[1:] != ["bootstrap"]:
    raise SystemExit("expected bootstrap")
runner = Path(os.environ["FORGEJO_RUNNER_DIRECTORY"])
runner.mkdir(parents=True, exist_ok=True)
(runner / "registry-stub.log").write_text("bootstrap\n", encoding="utf-8")
EOF
}

setup_case() {
  case_directory="$temporary_root/$1"
  mkdir -p "$case_directory/seeds/gateway-c37-118-onboarding"
  printf '%s\n' gateway-c37-118-onboarding > "$case_directory/seeds/gateway-c37-118-onboarding/README.md"
  : > "$case_directory/app.ini"
  : > "$case_directory/forgejo.log"
  : > "$case_directory/git.log"
  : > "$case_directory/curl.log"
  : > "$case_directory/wget.log"
  create_fake_commands "$case_directory/bin"
  create_registry_stub "$case_directory/registry.py"
}

run_bootstrap() {
  PATH="$case_directory/bin:$PATH" \
    FORGEJO_SERVER_CONFIG="$case_directory/app.ini" \
    FORGEJO_RUNNER_DIRECTORY="$case_directory/runner" \
    FORGEJO_PROCESSOR_SEED_ROOT="$case_directory/seeds" \
    FORGEJO_API_URL=http://forgejo.test/api/v1 \
    FORGEJO_INTERNAL_ROOT_URL=http://forgejo.test \
    FORGEJO_BOOTSTRAP_ADMIN_USERNAME=wama-admin \
    FORGEJO_BOOTSTRAP_ADMIN_PASSWORD=wama-admin \
    FORGEJO_GATEWAY_C37_118_ONBOARDING_AGENT_USERNAME=wama-gateway-onboarding-agent \
    FORGEJO_GATEWAY_C37_118_ONBOARDING_AGENT_EMAIL=wama-gateway-onboarding-agent@test \
    FORGEJO_RUNNER_URL=http://forgejo.test/ \
    WAMA_PROCESSOR_DEPLOY_BASE_ROOT="$case_directory/processor-deployments" \
    WAMA_GATEWAY_C37_118_ONBOARDING_DEPLOY_ROOT="$case_directory/gateway-deploy" \
    WAMA_PROCESSOR_REGISTRY_SCRIPT="$case_directory/registry.py" \
    BOOTSTRAP_TEST_AGENT_USER_FILE="$case_directory/gateway-agent-user" \
    BOOTSTRAP_TEST_CURL_LOG="$case_directory/curl.log" \
    BOOTSTRAP_TEST_FORGEJO_LOG="$case_directory/forgejo.log" \
    BOOTSTRAP_TEST_GIT_LOG="$case_directory/git.log" \
    BOOTSTRAP_TEST_WGET_LOG="$case_directory/wget.log" \
    sh "$bootstrap_script"
}

assert_contains() {
  expected="$1"
  path="$2"
  grep -Fq -- "$expected" "$path" || {
    printf '%s\n' "Expected $path to contain $expected" >&2
    exit 1
  }
}

assert_not_contains() {
  unexpected="$1"
  path="$2"
  if grep -Fq -- "$unexpected" "$path"; then
    printf '%s\n' "Expected $path not to contain $unexpected" >&2
    exit 1
  fi
}

test_bootstraps_gateway_and_delegates_processor_registry() {
  setup_case bootstrap
  export BOOTSTRAP_TEST_REPOSITORY_EXISTS=false
  export BOOTSTRAP_TEST_REPOSITORY_REFS=
  run_bootstrap > "$case_directory/bootstrap.log" 2>&1
  assert_contains gateway-c37-118-onboarding.git "$case_directory/git.log"
  assert_not_contains processor-frequency-scale.git "$case_directory/git.log"
  assert_contains "--name wama-gateway-c37-118-onboarding-ci" "$case_directory/forgejo.log"
  assert_contains "--name wama-gateway-c37-118-onboarding-deploy" "$case_directory/forgejo.log"
  assert_contains bootstrap "$case_directory/runner/registry-stub.log"
  test -f "$case_directory/gateway-deploy/.wama-forgejo-gateway-onboarding-root"
  assert_contains 'owner=wama-admin' "$case_directory/runner/forgejo-gateway-c37-118-onboarding-agent.identity"
  assert_contains 'repository=gateway-c37-118-onboarding' "$case_directory/runner/forgejo-gateway-c37-118-onboarding-agent.identity"
  assert_contains 'username=wama-gateway-onboarding-agent' "$case_directory/runner/forgejo-gateway-c37-118-onboarding-agent.identity"
  test "$(stat -c '%a' "$case_directory/runner/forgejo-gateway-c37-118-onboarding-agent.token")" = 600
  run_bootstrap >> "$case_directory/bootstrap.log" 2>&1
  test "$(grep -Fc -- "admin user create --username wama-gateway-onboarding-agent" "$case_directory/forgejo.log")" -eq 1
}

test_skips_nonempty_gateway_repository() {
  setup_case skip-nonempty
  export BOOTSTRAP_TEST_REPOSITORY_EXISTS=true
  export BOOTSTRAP_TEST_REPOSITORY_REFS="deadbeef refs/heads/main"
  run_bootstrap > "$case_directory/bootstrap.log" 2>&1
  if grep -Eq '(^| )(init|commit|push)( |$)' "$case_directory/git.log"; then
    printf '%s\n' "Bootstrap changed a repository with refs" >&2
    exit 1
  fi
  assert_contains "gateway-c37-118-onboarding already has refs; leaving it unchanged" "$case_directory/bootstrap.log"
}

test_rejects_unmarked_nonempty_gateway_root() {
  setup_case reject-gateway-root
  mkdir "$case_directory/gateway-deploy"
  printf '%s\n' unmanaged > "$case_directory/gateway-deploy/file"
  export BOOTSTRAP_TEST_REPOSITORY_EXISTS=true
  export BOOTSTRAP_TEST_REPOSITORY_REFS="deadbeef refs/heads/main"
  if run_bootstrap > "$case_directory/bootstrap.log" 2>&1; then
    printf '%s\n' "Bootstrap accepted an unmarked nonempty gateway root" >&2
    exit 1
  fi
  assert_contains "must be empty before bootstrap creates its marker" "$case_directory/bootstrap.log"
}

test_bootstraps_gateway_and_delegates_processor_registry
test_skips_nonempty_gateway_repository
test_rejects_unmarked_nonempty_gateway_root