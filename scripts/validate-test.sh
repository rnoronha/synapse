#!/bin/bash
# validate-test.sh — Pre-flight checks for Synapse perf test scripts.
# Automates checklist sections 1-3 from .kiro/analysis/test-review-checklist.md
#
# Usage:
#   bash scripts/validate-test.sh scripts/test_throughput.py
#   bash scripts/validate-test.sh --all

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEPLOY_SCRIPT="$SCRIPT_DIR/deploy-and-test.sh"

RED='\033[0;31m'
YEL='\033[0;33m'
GRN='\033[0;32m'
RST='\033[0m'

PASS=0
FAIL=0
WARN=0

pass() { PASS=$((PASS + 1)); printf "  ${GRN}✓${RST} %s\n" "$1"; }
fail() { FAIL=$((FAIL + 1)); printf "  ${RED}✗ FAIL:${RST} %s\n" "$1"; }
warn() { WARN=$((WARN + 1)); printf "  ${YEL}⚠ WARN:${RST} %s\n" "$1"; }

# ── Section 1: Pre-Run Validation ─────────────────────────────────────

check_compiles() {
    local script="$1"
    if python3 -c "import py_compile; py_compile.compile('$script', doraise=True)" 2>/dev/null; then
        pass "Compiles"
    else
        fail "Does not compile: $script"
    fi
}

check_help() {
    local script="$1"
    if timeout 15 python3 "$script" --help >/dev/null 2>&1; then
        pass "--help exits 0"
    else
        fail "--help fails or hangs"
    fi
}

check_hardcoded_durations() {
    local script="$1"
    local overrides
    overrides=$(grep -nP 'override_dur\s*=\s*(?!None)\d+' "$script" 2>/dev/null || true)
    if [[ -n "$overrides" ]]; then
        fail "Hardcoded override_dur ignores --duration flag"
        echo "      $overrides"
    else
        local big_sleeps
        big_sleeps=$(grep -nP 'asyncio\.sleep\(\s*[0-9]{3,}' "$script" 2>/dev/null || true)
        if [[ -n "$big_sleeps" ]]; then
            warn "Large hardcoded sleep (>99s) — verify it respects --duration"
        else
            pass "No hardcoded duration overrides"
        fi
    fi
}

check_signal_handling() {
    local script="$1"
    if grep -qP 'signal\.(SIGTERM|SIGINT)' "$script" 2>/dev/null; then
        pass "Handles SIGTERM/SIGINT"
    else
        warn "No signal handling — may not produce partial results on timeout"
    fi
}

# ── Section 2: Assertion Validation ───────────────────────────────────

check_can_fail() {
    local script="$1"
    # Direct sys.exit(1), or return 1 from main() feeding sys.exit(asyncio.run(main()))
    if grep -qP 'sys\.exit\(1\)|sys\.exit\(.* else 1\)|exit\(1\)|raise (AssertionError|SystemExit)|return 1' "$script" 2>/dev/null; then
        pass "Can exit non-zero on failure"
    else
        fail "No sys.exit(1) or equivalent — test always passes"
    fi
}

check_vacuous_pass() {
    local script="$1"
    if grep -qiP 'vacuous|total.*==.*0.*fail|0 results' "$script" 2>/dev/null; then
        pass "Detects vacuous passes (0 results)"
    else
        fail "No vacuous pass detection — 0 results = silent PASS"
    fi
}

check_broad_except() {
    local script="$1"
    local count
    count=$(grep -cP '^\s{4,}except\s+(Exception|BaseException)\b' "$script" 2>/dev/null || true)
    count=$(echo "$count" | head -1 | tr -d '[:space:]')
    count=${count:-0}
    if [[ "$count" -gt 2 ]]; then
        warn "$count broad exception catches — verify each is deliberate"
    else
        pass "Exception handling looks targeted ($count broad catches)"
    fi
}

# ── Section 3: Infrastructure Validation ──────────────────────────────

check_deploy_case() {
    local test_name="$1"
    if grep -qP "^\s+${test_name}\)" "$DEPLOY_SCRIPT" 2>/dev/null; then
        pass "deploy-and-test.sh has case for '$test_name'"
    else
        fail "deploy-and-test.sh missing case for '$test_name'"
    fi
}

check_duration_passthrough() {
    local test_name="$1"
    local script="$2"
    if ! grep -qP 'add_argument.*--duration' "$script" 2>/dev/null; then
        pass "Script doesn't accept --duration (N/A)"
        return
    fi
    local cmd_block
    cmd_block=$(sed -n "/^\s\+${test_name})/,/;;/p" "$DEPLOY_SCRIPT" 2>/dev/null || true)
    if echo "$cmd_block" | grep -q 'DURATION\|--duration'; then
        pass "--duration passed through in deploy-and-test.sh"
    else
        fail "deploy-and-test.sh does NOT pass --duration to $test_name"
    fi
}

check_ssm_timeout() {
    local test_name="$1"
    local script="$2"
    if grep -qP 'CONDITIONS\s*=' "$script" 2>/dev/null; then
        local cond_count
        cond_count=$(python3 -c "
import ast, sys
with open('$script') as f:
    tree = ast.parse(f.read())
for node in ast.walk(tree):
    if isinstance(node, ast.Assign):
        for t in node.targets:
            if isinstance(t, ast.Name) and t.id == 'CONDITIONS':
                if isinstance(node.value, ast.List):
                    print(len(node.value.elts))
                    sys.exit(0)
print(1)
" 2>/dev/null || echo 1)
        if [[ "$cond_count" -gt 1 ]]; then
            # Check if deploy-and-test.sh has special handling
            if grep -A5 "test.*==.*\"${test_name}\"" "$DEPLOY_SCRIPT" 2>/dev/null | grep -q 'SSM_TIMEOUT'; then
                pass "SSM timeout has special handling for multi-condition test ($cond_count conditions)"
            else
                warn "Test has $cond_count conditions — verify SSM_TIMEOUT = $cond_count × DURATION + 300"
            fi
        else
            pass "Single-condition test — default SSM timeout OK"
        fi
    else
        pass "No CONDITIONS list — default SSM timeout OK"
    fi
}

check_output_flag() {
    local test_name="$1"
    local cmd_block
    cmd_block=$(sed -n "/^\s\+${test_name})/,/;;/p" "$DEPLOY_SCRIPT" 2>/dev/null || true)
    if echo "$cmd_block" | grep -q 'OUTPUT_FLAG\|--output'; then
        pass "Results written to file (not just stdout)"
    else
        fail "No --output flag — results may be lost to 24KB SSM cap"
    fi
}

# ── Main ──────────────────────────────────────────────────────────────

validate_one() {
    local script="$1"
    local basename
    basename=$(basename "$script" .py)
    local test_name
    test_name=$(echo "$basename" | sed 's/^test_//' | tr '_' '-')

    echo ""
    echo "═══════════════════════════════════════════════════════════"
    echo "  Validating: $script  (test name: $test_name)"
    echo "═══════════════════════════════════════════════════════════"

    echo ""
    echo "── Section 1: Pre-Run Validation ──────────────────────────"
    check_compiles "$script"
    check_help "$script"
    check_hardcoded_durations "$script"
    check_signal_handling "$script"

    echo ""
    echo "── Section 2: Assertion Validation ────────────────────────"
    check_can_fail "$script"
    check_vacuous_pass "$script"
    check_broad_except "$script"

    echo ""
    echo "── Section 3: Infrastructure Validation ───────────────────"
    check_deploy_case "$test_name"
    check_duration_passthrough "$test_name" "$script"
    check_ssm_timeout "$test_name" "$script"
    check_output_flag "$test_name"
}

# Parse args
TARGETS=()
while [[ $# -gt 0 ]]; do
    case $1 in
        --all)
            for f in "$SCRIPT_DIR"/test_*.py; do
                case "$(basename "$f")" in
                    test_query_*|test_reader_*|test_local_*) continue;;
                esac
                TARGETS+=("$f")
            done
            shift;;
        *)
            if [[ -f "$1" ]]; then
                TARGETS+=("$1")
            else
                echo "File not found: $1"
                exit 1
            fi
            shift;;
    esac
done

if [[ ${#TARGETS[@]} -eq 0 ]]; then
    echo "Usage: $0 <script.py> | --all"
    echo ""
    echo "Validates test scripts against the test review checklist."
    echo "Automates sections 1-3 (pre-run, assertion, infrastructure)."
    exit 1
fi

for target in "${TARGETS[@]}"; do
    validate_one "$target"
done

echo ""
echo "═══════════════════════════════════════════════════════════"
printf "  Results: ${GRN}%d passed${RST}, ${RED}%d failed${RST}, ${YEL}%d warnings${RST}\n" "$PASS" "$FAIL" "$WARN"
echo "═══════════════════════════════════════════════════════════"

if [[ $FAIL -gt 0 ]]; then
    echo ""
    echo "Fix all FAIL items before running tests on EC2."
    exit 1
fi
