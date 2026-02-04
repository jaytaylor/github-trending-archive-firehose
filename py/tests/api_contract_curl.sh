#!/usr/bin/env bash
set -euo pipefail

BASE_URL="http://127.0.0.1:8000"
SCENARIO="happy"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base-url)
      BASE_URL="$2"
      shift 2
      ;;
    --scenario)
      SCENARIO="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

OUT_DIR=".scratch/verification/SPRINT-001/001E-2/responses"
mkdir -p "$OUT_DIR"

check_content_type() {
  local headers_file="$1"
  if ! rg -qi "content-type: application/json" "$headers_file"; then
    echo "Missing JSON content-type in $headers_file" >&2
    exit 1
  fi
}

curl_json() {
  local name="$1"
  local url="$2"
  local headers_file="$OUT_DIR/${name}.headers"
  local body_file="$OUT_DIR/${name}.json"
  curl -sS -D "$headers_file" "$url" -o "$body_file"
  check_content_type "$headers_file"
}

case "$SCENARIO" in
  happy)
    curl_json "dates" "$BASE_URL/api/v1/dates?kind=repository"
    curl_json "day" "$BASE_URL/api/v1/day?kind=repository&date=2025-01-01&language=python"
    curl_json "top_reappearing" "$BASE_URL/api/v1/top/reappearing?kind=repository&start=2025-01-01&end=2025-01-02&presence=day&include_all_languages=false&limit=5"
    curl_json "top_owners" "$BASE_URL/api/v1/top/owners?start=2025-01-01&end=2025-01-02&include_all_languages=false&limit=5"
    curl_json "top_languages" "$BASE_URL/api/v1/top/languages?start=2025-01-01&end=2025-01-02&kind=repository&include_all_languages=false&limit=5"
    curl_json "top_streaks" "$BASE_URL/api/v1/top/streaks?kind=repository&start=2025-01-01&end=2025-01-02&include_all_languages=false&limit=5"
    curl_json "top_newcomers" "$BASE_URL/api/v1/top/newcomers?kind=repository&start=2025-01-01&end=2025-01-02&include_all_languages=false&limit=5"
    ;;
  invalid-date)
    curl_json "invalid_date" "$BASE_URL/api/v1/day?kind=repository&date=2025-13-01&language=python"
    ;;
  missing-date)
    curl_json "missing_date" "$BASE_URL/api/v1/day?kind=repository&date=1900-01-01&language=python"
    ;;
  *)
    echo "Unknown scenario: $SCENARIO" >&2
    exit 2
    ;;
esac
