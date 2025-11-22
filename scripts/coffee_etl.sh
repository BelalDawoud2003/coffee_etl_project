#!/bin/bash
set -euo pipefail
IFS=$'\n\t'

# ==========================================================
# Configuration
# ==========================================================
DB_USER="etl_us"
DB_PASS="pass_us"
DB_NAME="coffeeshop"

BASE_DIR="$(cd "$(dirname "$0")" && pwd)/.."
RAW_DIR="$BASE_DIR/data/raw"
PROCESSED_DIR="$BASE_DIR/processed"
LOG_DIR="$BASE_DIR/logs"
REPORT_DIR="$BASE_DIR/reports"

LOG_FILE="$LOG_DIR/etl_$(date +%F).log"
ERROR_FILE="$LOG_DIR/error_$(date +%F).log"

ALERT_EMAIL="fatma140mohamed@gmail.com"
KEEP_LOG_DAYS=7

mkdir -p "$RAW_DIR" "$PROCESSED_DIR" "$LOG_DIR" "$REPORT_DIR"

# ==========================================================
# Helper Functions
# ==========================================================
log() {
  echo "$(date '+%F %T') - $1" | tee -a "$LOG_FILE"
}

handle_error() {
  local msg="$1"
  echo "$(date '+%F %T') - ERROR: $msg" | tee -a "$ERROR_FILE"
  send_alert "ETL Failed" "$msg"
  exit 1
}

run_step() {
  log "Running: $1"
  eval "$1"
  local rc=$?
  if [ $rc -ne 0 ]; then
    handle_error "Step failed: $1"
  fi
}

# ==========================================================
# Preflight
# ==========================================================
preflight() {
  log "Running preflight checks..."
  command -v jq >/dev/null 2>&1 || log "Warning: jq not installed."
  command -v mysql >/dev/null 2>&1 || log "Warning: MySQL client not installed."
  if command -v mailx >/dev/null 2>&1; then
    MAIL_CMD="mailx"
  else
    log "Warning: mailx not installed. Email alerts will not be sent."
  fi
  log "Preflight done."
}

send_email() {
  local subject="$1"
  local body="$2"
  if [ -z "${MAIL_CMD:-}" ]; then
    log "Skipping email alert: mailx not available."
    return
  fi
  $MAIL_CMD -s "$subject" "$ALERT_EMAIL" <<< "$body" 2>>"$ERROR_FILE" && log "Alert email sent."
}

send_alert() {
  local subject="$1"
  local body="$2"
  send_email "$subject" "$body" &
}

# ==========================================================
# Extraction
# ==========================================================
extract_json() {
  log "Extracting JSON data..."
  cp "$BASE_DIR/data/online_orders.json" "$RAW_DIR/online_orders.json" \
    && log "JSON copied to $RAW_DIR/online_orders.json" \
    || handle_error "Failed to copy online_orders.json"
}

extract_csv() {
  log "Extracting CSV data..."
  cp "$BASE_DIR/data/instore_sales.csv" "$RAW_DIR/instore_sales.csv" \
    && log "CSV copied to $RAW_DIR/instore_sales.csv" \
    || handle_error "Failed to copy instore_sales.csv"
}

extract_db() {
  log "Extracting store_inventory from DB..."
  mysql -u "$DB_USER" -p"$DB_PASS" -D "$DB_NAME" -e "SELECT * FROM store_inventory;" \
    > "$RAW_DIR/store_inventory.csv" 2>>"$ERROR_FILE" \
    && log "DB extraction complete: $RAW_DIR/store_inventory.csv" \
    || handle_error "DB extraction failed"
}

# ==========================================================
# Transformation
# ==========================================================
transform_json() {
  log "Transforming JSON to CSV..."
  local INPUT="$RAW_DIR/online_orders.json"
  local OUTPUT="$PROCESSED_DIR/online_orders.csv"
  echo "id,product,category,price,quantity,source,total_sales" > "$OUTPUT"
  jq -r '.[] | [.id, .product, .category, .price, .quantity] | @csv' "$INPUT" \
    | sed 's/"//g' \
    | awk -F',' -v OFS=',' '{ if($4>0 && $5>0) print $0,"online",$4*$5 }' \
    >> "$OUTPUT"
  log "JSON transformed to $OUTPUT"
}

transform_csv() {
  log "Processing instore CSV..."
  local INPUT="$RAW_DIR/instore_sales.csv"
  local OUTPUT="$PROCESSED_DIR/instore_sales.csv"
  echo "id,product,category,price,quantity,source,total_sales" > "$OUTPUT"
  tail -n +2 "$INPUT" | awk -F',' -v OFS=',' '{ if($3>0 && $4>0) print $1,$2,$3,$4,$5,"instore",$4*$5 }' >> "$OUTPUT"
  log "CSV transformed to $OUTPUT"
}

transform_db() {
  log "Transforming DB CSV..."
  local INPUT="$RAW_DIR/store_inventory.csv"
  local OUTPUT="$PROCESSED_DIR/store_inventory_clean.csv"
  echo "id,product,category,price,quantity,source,total_sales" > "$OUTPUT"
  tail -n +2 "$INPUT" | awk -F'\t' -v OFS=',' '{ if($3>0 && $4>0) print $1,$2,$3,$4,$5,"inventory",$4*$5 }' >> "$OUTPUT"
  log "DB CSV transformed to $OUTPUT"
}

# ==========================================================
# Loading & Merging
# ==========================================================
load_data() {
  log "Merging all processed CSVs..."
  local OUT="$PROCESSED_DIR/final_output.csv"
  local TMP="$PROCESSED_DIR/final_temp.csv"
  > "$TMP"

  for f in "$PROCESSED_DIR"/*.csv; do
    [ -f "$f" ] || continue
    if [ ! -s "$TMP" ]; then
      cat "$f" >> "$TMP"
    else
      tail -n +2 "$f" >> "$TMP"
    fi
  done

  if [ -s "$TMP" ]; then
    { read header; echo "record_id,$header"; awk -F',' -v OFS=',' '{print NR-1 "," $0}' "$TMP"; } < "$TMP" > "$OUT"
    log "Final merged CSV created: $OUT"
  else
    handle_error "No data to merge"
  fi
  rm -f "$TMP"
}

archive_old() {
  log "Archiving processed CSVs..."
  tar -czf "$PROCESSED_DIR/archive_$(date +%F).tar.gz" "$PROCESSED_DIR"/*.csv || log "No CSV files to archive"
}

# ==========================================================
# Reporting
# ==========================================================
generate_report() {
  log "Generating summary report..."
  local REPORT="$REPORT_DIR/report_$(date +%F).txt"
  echo "---- ETL Summary $(date) ----" > "$REPORT"

  # Revenue by category
  awk -F',' 'NR>1{arr[$3]+=$7} END{for(c in arr) print c,arr[c]}' "$PROCESSED_DIR/final_output.csv" \
    >> "$REPORT"

  # Top products
  awk -F',' 'NR>1{arr[$2]+=$7} END{for(p in arr) print p,arr[p]}' "$PROCESSED_DIR/final_output.csv" \
    | sort -k2 -nr | head -n10 >> "$REPORT"

  # Low inventory (quantity < 5)
  awk -F',' 'NR>1 && $6=="inventory" && $5<5{print $2,$5}' "$PROCESSED_DIR/final_output.csv" \
    >> "$REPORT"

  log "Report created: $REPORT"
}

cleanup_logs() {
  log "Cleaning logs older than $KEEP_LOG_DAYS days..."
  find "$LOG_DIR" -type f -mtime +$KEEP_LOG_DAYS -delete || true
  log "Log cleanup done."
}

# ==========================================================
# Main Workflow
# ==========================================================
main() {
  preflight
  log "Starting ETL pipeline..."

  extract_json &
  extract_csv &
  extract_db &
  wait
  log "All extraction steps completed."

  run_step "transform_json"
  run_step "transform_csv"
  run_step "transform_db"
  run_step "load_data"
  run_step "archive_old"
  run_step "generate_report"
  run_step "cleanup_logs"

  send_alert "ETL Success" "Pipeline completed successfully on $(date)"
  log "ETL pipeline completed successfully!"
}

trap 'handle_error "Unexpected error on line $LINENO"' ERR
main
