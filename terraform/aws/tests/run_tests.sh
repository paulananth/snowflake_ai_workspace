#!/usr/bin/env bash
# terraform/aws/tests/run_tests.sh
#
# Master test runner for the SEC EDGAR Bronze Terraform module.
# Runs all test stages in order from fastest/cheapest to slowest.
#
# Stages:
#   1. Prerequisites check  — verify required tools are installed
#   2. terraform fmt        — formatting check (no credentials needed)
#   3. terraform validate   — syntax + reference check (no credentials needed)
#   4. tflint               — provider-aware linting (no credentials needed)
#   5. trivy                — security/misconfiguration scan (no credentials needed)
#   6. terraform test       — plan-level assertions using mock provider (no credentials needed)
#
# Usage:
#   cd terraform/aws
#   bash tests/run_tests.sh
#
# Exit codes:
#   0  all stages passed
#   1  one or more stages failed
#
# Skipping stages:
#   SKIP_TFLINT=1  bash tests/run_tests.sh   # skip tflint if not installed
#   SKIP_TRIVY=1   bash tests/run_tests.sh   # skip trivy if not installed
#   SKIP_TFLINT=1 SKIP_TRIVY=1 bash tests/run_tests.sh

set -uo pipefail

# ---------------------------------------------------------------------------
# Resolve paths
# ---------------------------------------------------------------------------
TESTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TF_DIR="$(dirname "$TESTS_DIR")"          # terraform/aws/
REPO_ROOT="$(dirname "$(dirname "$TF_DIR")")"  # repo root

# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
RESET='\033[0m'

PASS="${GREEN}[PASS]${RESET}"
FAIL="${RED}[FAIL]${RESET}"
SKIP="${YELLOW}[SKIP]${RESET}"
INFO="${CYAN}[INFO]${RESET}"

FAILED_STAGES=()
PASSED_STAGES=()
SKIPPED_STAGES=()

log()  { echo -e "${CYAN}[$(date '+%H:%M:%S')]${RESET} $*"; }
pass() { echo -e "${PASS} $*"; PASSED_STAGES+=("$1"); }
fail() { echo -e "${FAIL} $*"; FAILED_STAGES+=("$1"); }
skip() { echo -e "${SKIP} $*"; SKIPPED_STAGES+=("$1"); }

# Run a command, stream its output, and record pass/fail for the stage
run_stage() {
    local stage_name="$1"
    shift
    echo ""
    echo -e "${BOLD}━━━ ${stage_name} ━━━${RESET}"
    if "$@"; then
        pass "$stage_name"
    else
        fail "$stage_name"
    fi
}

# ---------------------------------------------------------------------------
# Stage 0: Prerequisites
# ---------------------------------------------------------------------------
echo ""
echo -e "${BOLD}=================================================${RESET}"
echo -e "${BOLD}  SEC EDGAR Terraform Test Suite${RESET}"
echo -e "${BOLD}=================================================${RESET}"
log "Working directory: $TF_DIR"
echo ""
echo -e "${BOLD}━━━ Prerequisites ━━━${RESET}"

check_tool() {
    local tool="$1"
    local install_hint="$2"
    if command -v "$tool" &>/dev/null; then
        local version
        version=$("$tool" version 2>/dev/null | head -1 || echo "unknown version")
        echo -e "  ${GREEN}✓${RESET} $tool  ($version)"
        return 0
    else
        echo -e "  ${RED}✗${RESET} $tool not found.  Install: $install_hint"
        return 1
    fi
}

PREREQS_OK=true

check_tool "terraform" "https://developer.hashicorp.com/terraform/install" || PREREQS_OK=false

# Check Terraform version >= 1.7 (required for mock_provider in terraform test)
if command -v terraform &>/dev/null; then
    TF_VERSION=$(terraform version -json 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)['terraform_version'])" 2>/dev/null || terraform version | head -1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')
    TF_MAJOR=$(echo "$TF_VERSION" | cut -d. -f1)
    TF_MINOR=$(echo "$TF_VERSION" | cut -d. -f2)
    if [ "$TF_MAJOR" -lt 1 ] || { [ "$TF_MAJOR" -eq 1 ] && [ "$TF_MINOR" -lt 7 ]; }; then
        echo -e "  ${RED}✗${RESET} Terraform $TF_VERSION is too old. mock_provider requires >= 1.7"
        PREREQS_OK=false
    else
        echo -e "  ${GREEN}✓${RESET} Terraform $TF_VERSION >= 1.7 (mock_provider supported)"
    fi
fi

# Optional tools — warn but don't abort
TFLINT_AVAILABLE=false
TRIVY_AVAILABLE=false

if command -v tflint &>/dev/null; then
    TFLINT_AVAILABLE=true
    tflint --version | head -1 | awk '{print "  ✓ tflint  (" $0 ")"}'
else
    echo -e "  ${YELLOW}⚠${RESET}  tflint not found (optional). Install: brew install tflint"
fi

if command -v trivy &>/dev/null; then
    TRIVY_AVAILABLE=true
    trivy --version 2>/dev/null | head -1 | awk '{print "  ✓ trivy   (" $0 ")"}'
else
    echo -e "  ${YELLOW}⚠${RESET}  trivy not found (optional). Install: brew install trivy"
fi

if [ "$PREREQS_OK" = false ]; then
    echo ""
    echo -e "${RED}Required prerequisites missing. Fix the above errors and re-run.${RESET}"
    exit 1
fi

cd "$TF_DIR"

# ---------------------------------------------------------------------------
# Stage 1: terraform init (required before validate / test)
# ---------------------------------------------------------------------------
run_stage "terraform init" \
    terraform init -backend=false -input=false -no-color

# ---------------------------------------------------------------------------
# Stage 2: terraform fmt
# ---------------------------------------------------------------------------
fmt_check() {
    local out
    out=$(terraform fmt -check -recursive -no-color . 2>&1)
    local rc=$?
    if [ $rc -ne 0 ]; then
        echo "Formatting issues in:"
        echo "$out" | sed 's/^/  /'
        echo ""
        echo "  Fix with: terraform fmt -recursive terraform/aws/"
    fi
    return $rc
}
run_stage "terraform fmt" fmt_check

# ---------------------------------------------------------------------------
# Stage 3: terraform validate
# ---------------------------------------------------------------------------
run_stage "terraform validate" \
    terraform validate -no-color

# ---------------------------------------------------------------------------
# Stage 4: tflint
# ---------------------------------------------------------------------------
if [ "${SKIP_TFLINT:-0}" = "1" ]; then
    skip "tflint  (SKIP_TFLINT=1)"
elif [ "$TFLINT_AVAILABLE" = false ]; then
    skip "tflint  (not installed)"
else
    tflint_run() {
        tflint --init --no-color 2>/dev/null || true   # update plugins
        tflint --no-color
    }
    run_stage "tflint" tflint_run
fi

# ---------------------------------------------------------------------------
# Stage 5: trivy (IaC misconfiguration scan)
# ---------------------------------------------------------------------------
if [ "${SKIP_TRIVY:-0}" = "1" ]; then
    skip "trivy  (SKIP_TRIVY=1)"
elif [ "$TRIVY_AVAILABLE" = false ]; then
    skip "trivy  (not installed)"
else
    trivy_run() {
        # --exit-code 1 makes trivy exit non-zero on HIGH/CRITICAL findings
        trivy config \
            --exit-code 1 \
            --severity HIGH,CRITICAL \
            --no-progress \
            .
    }
    run_stage "trivy config" trivy_run
fi

# ---------------------------------------------------------------------------
# Stage 6: terraform test (plan-level, mock provider — no AWS credentials)
# ---------------------------------------------------------------------------
run_stage "terraform test" \
    terraform test -no-color

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo -e "${BOLD}=================================================${RESET}"
echo -e "${BOLD}  Test Results${RESET}"
echo -e "${BOLD}=================================================${RESET}"

for s in "${PASSED_STAGES[@]+"${PASSED_STAGES[@]}"}"; do
    echo -e "  ${GREEN}✓ PASS${RESET}  $s"
done

for s in "${SKIPPED_STAGES[@]+"${SKIPPED_STAGES[@]}"}"; do
    echo -e "  ${YELLOW}⚠ SKIP${RESET}  $s"
done

for s in "${FAILED_STAGES[@]+"${FAILED_STAGES[@]}"}"; do
    echo -e "  ${RED}✗ FAIL${RESET}  $s"
done

echo ""
TOTAL=$(( ${#PASSED_STAGES[@]} + ${#SKIPPED_STAGES[@]} + ${#FAILED_STAGES[@]} ))
echo -e "  Stages: ${TOTAL} total  |  ${#PASSED_STAGES[@]} passed  |  ${#SKIPPED_STAGES[@]} skipped  |  ${#FAILED_STAGES[@]} failed"
echo ""

if [ ${#FAILED_STAGES[@]} -gt 0 ]; then
    echo -e "${RED}One or more test stages failed.${RESET}"
    exit 1
else
    echo -e "${GREEN}All test stages passed.${RESET}"
    exit 0
fi
