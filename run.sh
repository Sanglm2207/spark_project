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

run_min_temperature() {
    header "Job: Minimum Temperature"
    python -m src.jobs.min_temperatures
}

run_flight_source_sink() {
    header "Job: Flight Source and Sink"
    python -m src.jobs.flight_source_sink
}

# Lab 4
run_hello_spark_sql() {
    header "Job: Hello SparkSQL"
    python -m src.jobs.hello_spark_sql
}

run_spark_sql_table_demo() {
    header "Job: SparkSQL Table Demo"
    python -m src.jobs.spark_sql_table_demo
}

# Lab 5
row_demo() {
    header "Job: Row Demo"
    python -m src.jobs.row_demo
}

run_log_file_demo() {
    header "Job: Log File Demo"
    python -m src.jobs.log_file_demo
}

run_column_demo() {
    header "Job: Column Demo"
    python -m src.jobs.column_demo
}

run_udf_demo() {
    header "Job: UDF Demo"
    python -m src.jobs.udf_demo
}

# Lab 6
run_agg_demo() {
    header "Job: Aggregation Demo"
    python -m src.jobs.agg_demo
}

run_windowing_demo() {
    header "Job: Window Function Demo"
    python -m src.jobs.windowing_demo
}

run_join_demo() {
    header "Job: Join Demo"
    python -m src.jobs.join_demo
}

# Lab 7
run_most_popular_superhero() {
    header "Job: Most Popular Superhero"
    python -m src.jobs.most_popular_superhero
}

# Lab 8
run_file_stream_demo() {
    header "Job: File Stream Demo"
    python -m src.jobs.file_stream_demo
}

# Lab 9
run_multi_query_demo() {
    header "Job: Multi Query Demo (Kafka)"
    info "Đảm bảo Kafka đang chạy: docker-compose up -d"
    python -m src.jobs.multi_query_demo
}

run_kafka_producer() {
    header "Kafka Producer"
    info "Đảm bảo Kafka đang chạy: docker-compose up -d"
    python src/utils/kafka_producer.py
}

run_install() { 
    header "Cài dependencies"; 
    pip install -r requirements.txt;
    ok "Done!"; 
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
    echo -e "  ${CYAN}6${RESET}  Job: Minimum Temperatures"
    echo -e "  ${CYAN}7${RESET}  Job: Flight Source and Sink"
    echo -e "  ${CYAN}8${RESET}  Job: Hello SparkSQL"
    echo -e "  ${CYAN}9${RESET}  Job: SparkSQL Table Demo"
    echo -e "  ${CYAN}10${RESET} Job: Row Demo"
    echo -e "  ${CYAN}11${RESET} Job: Log File Demo"
    echo -e "  ${CYAN}12${RESET} Job: Column Demo"
    echo -e "  ${CYAN}13${RESET} Job: UDF Demo"
    echo -e "  ${CYAN}14${RESET} Job: Aggregation Demo"
    echo -e "  ${CYAN}15${RESET} Job: Window Function Demo"
    echo -e "  ${CYAN}16${RESET} Job: Join Demo"
    echo -e "  ${CYAN}17${RESET} Job: Most Popular Superhero"
    echo -e "  ${CYAN}18${RESET} Job: File Stream Demo"
    echo -e "  ${CYAN}19${RESET} Job: Multi Query Demo (Kafka)"
    echo -e "  ${CYAN}20${RESET} Kafka Producer"
    echo -e "  ${CYAN}21${RESET} Install dependencies"
    echo -e "  ${CYAN}22${RESET} Clean __pycache__"
    echo -e "  ${CYAN}q${RESET}  Exit \n"

    read -rp "  Chọn: " choice

    case "$choice" in
        1) run_check ;;
        2) run_word_count ;;
        3) run_sales_agg ;;
        4) run_rating_counter ;;
        5) run_avg_friends ;;
        6) run_min_temperature ;;
        7) run_flight_source_sink ;;
        8) run_hello_spark_sql ;;
        9) run_spark_sql_table_demo ;;
        10) row_demo ;;
        11) run_log_file_demo ;;
        12) run_column_demo ;;
        13) run_udf_demo ;;
        14) run_agg_demo ;;
        15) run_windowing_demo ;;
        16) run_join_demo ;;
        17) run_most_popular_superhero ;;
        18) run_file_stream_demo ;;
        19) run_multi_query_demo ;;
        20) run_kafka_producer ;;
        21) run_install ;;
        22) run_clean ;;
        q|Q) echo -e "\n  Bye!\n"; exit 0 ;;
        *) error "Không hợp lệ — chọn 1-22 hoặc q" ;;
    esac
done
