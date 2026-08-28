#!/bin/sh

set -eu

repository_root="$(cd "$(dirname "$0")/../../.." && pwd)"
temporary_root="$(mktemp -d)"
infrastructure_root="$temporary_root/infrastructure"
installer="$infrastructure_root/scripts/configure-forgejo-gateway-c37-118-agent.sh"
wrapper="$infrastructure_root/scripts/with-forgejo-gateway-c37-118-agent.sh"

cleanup() {
  rm -rf "$temporary_root"
}

trap cleanup EXIT HUP INT TERM

fail() {
  printf '%s\n' "$1" >&2
  exit 1
}

assert_contains() {
  expected="$1"
  path="$2"
  grep -Fq "$expected" "$path" || fail "Expected $path to contain $expected"
}

create_fake_docker() {
  fake_bin="$1"
  mkdir "$fake_bin"
  cat > "$fake_bin/docker" <<'EOF'
#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$GATEWAY_AGENT_TEST_DOCKER_LOG"
case "$*" in
  *forgejo-gateway-c37-118-agent.identity*)
    printf '%s\n' 'owner=wama-admin'
    printf '%s\n' 'repository=gateway-c37-118'
    printf '%s\n' 'username=wama-gateway-c37-118-agent'
    ;;
  *forgejo-gateway-c37-118-agent.token*)
    printf '%s\n' test-gateway-c37-118-agent-token
    ;;
  *) exit 1 ;;
esac
EOF
  chmod +x "$fake_bin/docker"
}

create_synthetic_infrastructure_root() {
  mkdir -p "$infrastructure_root/scripts" "$infrastructure_root/forgejo-repos"
  cp "$repository_root/scripts/configure-forgejo-gateway-c37-118-agent.sh" "$installer"
  cp "$repository_root/scripts/with-forgejo-gateway-c37-118-agent.sh" "$wrapper"
}

create_checkout() {
  checkout="$1"
  origin="$2"
  git init --quiet "$checkout"
  git -C "$checkout" remote add origin "$origin"
}

run_installer() {
  PATH="$temporary_root/bin:$PATH" \
    XDG_CONFIG_HOME="$temporary_root/config" \
    GATEWAY_AGENT_TEST_DOCKER_LOG="$temporary_root/docker.log" \
    sh "$installer" "$@"
}

test_configures_the_colocated_gateway_checkout() {
  : > "$temporary_root/docker.log"
  checkout="$infrastructure_root/forgejo-repos/gateway-c37-118"
  create_checkout "$checkout" http://forgejo.test/wama-admin/gateway-c37-118.git
  origin_before="$(git -C "$checkout" remote get-url origin)"
  run_installer > "$temporary_root/installer.log" 2>&1
  test "$(git -C "$checkout" remote get-url origin)" = "$origin_before"
  credential_file="$temporary_root/config/wama-forgejo/gateway-c37-118.credentials"
  test "$(stat -c '%a' "$temporary_root/config/wama-forgejo")" = 700
  test "$(stat -c '%a' "$credential_file")" = 600
  assert_contains 'test-gateway-c37-118-agent-token' "$credential_file"
  test "$(git -C "$checkout" config --local --get credential.useHttpPath)" = true
  test "$(git -C "$checkout" config --local --get credential.helper)" = "store --file=$credential_file"
  if grep -Fq test-gateway-c37-118-agent-token "$temporary_root/installer.log"; then
    fail "Installer wrote the C37.118 gateway agent token to its log"
  fi
  assert_contains forgejo-gateway-c37-118-agent.identity "$temporary_root/docker.log"
  assert_contains forgejo-gateway-c37-118-agent.token "$temporary_root/docker.log"
  wrapper_output="$(PATH="$temporary_root/bin:$PATH" \
    XDG_CONFIG_HOME="$temporary_root/config" \
    GATEWAY_AGENT_TEST_DOCKER_LOG="$temporary_root/docker.log" \
    FORGEJO_API_TOKEN=wrong-token \
    sh "$wrapper" -- sh -c '
      test "$FORGEJO_OWNER" = wama-admin
      test "$FORGEJO_REPOSITORY" = gateway-c37-118
      test "$FORGEJO_API_URL" = http://forgejo.test/api/v1
      test "$FORGEJO_AGENT_USERNAME" = wama-gateway-c37-118-agent
      test "$FORGEJO_API_TOKEN" != wrong-token
      test -n "$FORGEJO_API_TOKEN"
      printf configured
    ')"
  test "$wrapper_output" = configured
}

test_rejects_mismatched_agent_credential() {
  checkout="$temporary_root/mismatched-agent-checkout"
  create_checkout "$checkout" http://forgejo.test/wama-admin/gateway-c37-118.git
  run_installer --checkout "$checkout" > "$temporary_root/mismatched-installer.log" 2>&1
  credential_file="$temporary_root/config/wama-forgejo/gateway-c37-118.credentials"
  printf '%s\n' 'http://another-agent:another-token@forgejo.test/wama-admin/gateway-c37-118.git' \
    > "$credential_file"
  if PATH="$temporary_root/bin:$PATH" \
    XDG_CONFIG_HOME="$temporary_root/config" \
    GATEWAY_AGENT_TEST_DOCKER_LOG="$temporary_root/docker.log" \
    sh "$wrapper" --checkout "$checkout" -- true > "$temporary_root/mismatched-wrapper.log" 2>&1; then
    fail "Wrapper accepted a credential for a different Forgejo agent"
  fi
  assert_contains 'does not match its configured agent' "$temporary_root/mismatched-wrapper.log"
}

test_rejects_infrastructure_checkout_and_wrong_remote() {
  if run_installer --checkout "$infrastructure_root" > "$temporary_root/parent.log" 2>&1; then
    fail "Installer accepted the infrastructure checkout"
  fi
  wrong_checkout="$temporary_root/wrong-checkout"
  create_checkout "$wrong_checkout" http://forgejo.test/wama-admin/processor-frequency-scale.git
  if run_installer --checkout "$wrong_checkout" > "$temporary_root/wrong.log" 2>&1; then
    fail "Installer accepted an unrelated Forgejo repository"
  fi
}

create_synthetic_infrastructure_root
create_fake_docker "$temporary_root/bin"
test_configures_the_colocated_gateway_checkout
test_rejects_mismatched_agent_credential
test_rejects_infrastructure_checkout_and_wrong_remote