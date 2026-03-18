#!/usr/bin/env bash
# Chạy: chmod +x run.sh && ./run.sh

set -euo pipefail

# ── Colors ───────────────────────────────────────────────
GREEN="\033[92m"; YELLOW="\033[93m"; RED="\033[91m"
CYAN="\033[96m";  BOLD="\033[1m";    DIM="\033[2m"; RESET="\033[0m"

ok()     { echo -e "  ${GREEN}✔${RESET}  $*"; }
warn()   { echo -e "  ${YELLOW}⚠${RESET}  $*"; }
error()  { echo -e "  ${RED}✘${RESET}  $*"; }
info()   { echo -e "  ${CYAN}→${RESET}  $*"; }
header() { echo -e "\n${BOLD}$*${RESET}\n${DIM}$(printf '─%.0s' {1..52})${RESET}"; }

cd "$(dirname "$0")"

# ── Kiểm tra conda env ────────────────────────────────────
if [[ "${CONDA_DEFAULT_ENV:-}" != "spark-env" ]]; then
    warn "Chưa activate spark-env"
    info "Chạy:  conda activate spark-env  rồi thử lại"
    exit 1
fi

# ── Functions ─────────────────────────────────────────────
run_check() {
    header "Kiểm tra môi trường"
    python check_env.py
}

run_word_count() {
    header "Job: Word Count"
    python -m src.jobs.word_count
}

run_sales_agg() {
    header "Job: Sales Aggregation"
    python -m src.jobs.sales_agg
}

run_rating_counter() {
    header "Job: Ratings Histogram (MovieLens)"
    python -m src.jobs.ratings_counter
}

run_avg_friends() {
    header "Job: Average Friends by Age"
    python -m src.jobs.avg_friends_by_age
}

run_install() {
    header "Cài dependencies"
    pip install -r requirements.txt
    ok "Done!"
}

run_clean() {
    header "Dọn cache"
    find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
    find . -type f -name "*.pyc" -delete 2>/dev/null || true
    ok "Cleaned!"
}

# ── Menu ──────────────────────────────────────────────────
while true; do
    echo -e "\n${BOLD}DEP303x — PySpark Project${RESET}"
    echo -e "${DIM}$(printf '─%.0s' {1..52})${RESET}"
    echo -e "  ${CYAN}1${RESET}  Check: Environment"
    echo -e "  ${CYAN}2${RESET}  Job: Word Count"
    echo -e "  ${CYAN}3${RESET}  Job: Sales Aggregation"
    echo -e "  ${CYAN}4${RESET}  Job: Ratings Histogram (MovieLens)"
    echo -e "  ${CYAN}5${RESET}  Job: Average Friends by Age"
    echo -e "  ${CYAN}6${RESET}  Install dependencies"
    echo -e "  ${CYAN}7${RESET}  Clean __pycache__"
    echo -e "  ${CYAN}q${RESET}  Exit \n"

    read -rp "  Chọn: " choice

    case "$choice" in
        1) run_check ;;
        2) run_word_count ;;
        3) run_sales_agg ;;
        4) run_rating_counter ;;
        5) run_avg_friends ;;
        6) run_install ;;
        7) run_clean ;;
        q|Q) echo -e "\n  Bye!\n"; exit 0 ;;
        *) error "Không hợp lệ — chọn 1-7 hoặc q" ;;
    esac
done
