#!/bin/sh

set -eu

repository_root="$(cd "$(dirname "$0")/../../.." && pwd)"
installer="$repository_root/scripts/configure-forgejo-gateway-onboarding-agent.sh"
wrapper="$repository_root/scripts/with-forgejo-gateway-onboarding-agent.sh"
temporary_root="$(mktemp -d)"

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
  *forgejo-gateway-c37-118-onboarding-agent.identity*)
    printf '%s\n' 'owner=wama-admin'
    printf '%s\n' 'repository=gateway-c37-118-onboarding'
    printf '%s\n' 'username=wama-gateway-c37-118-onboarding-agent'
    ;;
  *forgejo-gateway-c37-118-onboarding-agent.token*)
    printf '%s\n' test-gateway-onboarding-agent-token
    ;;
  *) exit 1 ;;
esac
EOF
  chmod +x "$fake_bin/docker"
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
    sh "$installer" --checkout "$1"
}

test_configures_only_the_external_onboarding_checkout() {
  : > "$temporary_root/docker.log"
  checkout="$temporary_root/gateway-c37-118-onboarding"
  create_checkout "$checkout" http://forgejo.test/wama-admin/gateway-c37-118-onboarding.git
  origin_before="$(git -C "$checkout" remote get-url origin)"
  run_installer "$checkout" > "$temporary_root/installer.log" 2>&1
  test "$(git -C "$checkout" remote get-url origin)" = "$origin_before"
  credential_file="$temporary_root/config/wama-forgejo/gateway-c37-118-onboarding.credentials"
  test "$(stat -c '%a' "$temporary_root/config/wama-forgejo")" = 700
  test "$(stat -c '%a' "$credential_file")" = 600
  assert_contains 'test-gateway-onboarding-agent-token' "$credential_file"
  test "$(git -C "$checkout" config --local --get credential.useHttpPath)" = true
  test "$(git -C "$checkout" config --local --get credential.helper)" = "store --file=$credential_file"
  if grep -Fq test-gateway-onboarding-agent-token "$temporary_root/installer.log"; then
    fail "Installer wrote the gateway onboarding agent token to its log"
  fi
  assert_contains forgejo-gateway-c37-118-onboarding-agent.identity "$temporary_root/docker.log"
  assert_contains forgejo-gateway-c37-118-onboarding-agent.token "$temporary_root/docker.log"
  wrapper_output="$(PATH="$temporary_root/bin:$PATH" \
    XDG_CONFIG_HOME="$temporary_root/config" \
    GATEWAY_AGENT_TEST_DOCKER_LOG="$temporary_root/docker.log" \
    FORGEJO_API_TOKEN=wrong-token \
    sh "$wrapper" --checkout "$checkout" -- sh -c '
      test "$FORGEJO_OWNER" = wama-admin
      test "$FORGEJO_REPOSITORY" = gateway-c37-118-onboarding
      test "$FORGEJO_API_URL" = http://forgejo.test/api/v1
      test "$FORGEJO_AGENT_USERNAME" = wama-gateway-c37-118-onboarding-agent
      test "$FORGEJO_API_TOKEN" != wrong-token
      test -n "$FORGEJO_API_TOKEN"
      printf configured
    ')"
  test "$wrapper_output" = configured
}

test_rejects_mismatched_agent_credential() {
  checkout="$temporary_root/mismatched-agent-checkout"
  create_checkout "$checkout" http://forgejo.test/wama-admin/gateway-c37-118-onboarding.git
  run_installer "$checkout" > "$temporary_root/mismatched-installer.log" 2>&1
  credential_file="$temporary_root/config/wama-forgejo/gateway-c37-118-onboarding.credentials"
  printf '%s\n' 'http://another-agent:another-token@forgejo.test/wama-admin/gateway-c37-118-onboarding.git' \
    > "$credential_file"
  if PATH="$temporary_root/bin:$PATH" \
    XDG_CONFIG_HOME="$temporary_root/config" \
    GATEWAY_AGENT_TEST_DOCKER_LOG="$temporary_root/docker.log" \
    sh "$wrapper" --checkout "$checkout" -- true > "$temporary_root/mismatched-wrapper.log" 2>&1; then
    fail "Wrapper accepted a credential for a different Forgejo agent"
  fi
  assert_contains 'does not match its configured agent' "$temporary_root/mismatched-wrapper.log"
}

test_rejects_parent_seed_and_wrong_remote() {
  if run_installer "$repository_root" > "$temporary_root/parent.log" 2>&1; then
    fail "Installer accepted the infrastructure checkout"
  fi
  if run_installer "$repository_root/forgejo-repos/gateway-c37-118-onboarding" > "$temporary_root/seed.log" 2>&1; then
    fail "Installer accepted the tracked gateway onboarding seed"
  fi
  wrong_checkout="$temporary_root/wrong-checkout"
  create_checkout "$wrong_checkout" http://forgejo.test/wama-admin/processor-frequency-scale.git
  if run_installer "$wrong_checkout" > "$temporary_root/wrong.log" 2>&1; then
    fail "Installer accepted an unrelated Forgejo repository"
  fi
}

create_fake_docker "$temporary_root/bin"
test_configures_only_the_external_onboarding_checkout
test_rejects_mismatched_agent_credential
test_rejects_parent_seed_and_wrong_remote