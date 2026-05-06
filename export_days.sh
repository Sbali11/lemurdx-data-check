#!/bin/bash
# Export sensor data day-by-day to avoid Timestream timeouts
# Usage: ./export_days.sh <device_id> <measure_name> <start_date> <end_date> [output_dir]
# Example: ./export_days.sh 2005 motion_data 2025-11-15 2025-11-24 analysis

set -e

DEVICE_ID="${1:?Usage: $0 <device_id> <measure_name> <start_date> <end_date> [output_dir]}"
MEASURE_NAME="${2:?Usage: $0 <device_id> <measure_name> <start_date> <end_date> [output_dir]}"
START_DATE="${3:?Usage: $0 <device_id> <measure_name> <start_date> <end_date> [output_dir]}"
END_DATE="${4:?Usage: $0 <device_id> <measure_name> <start_date> <end_date> [output_dir]}"
OUTPUT_DIR="${5:-analysis}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON="/home/shreya/miniconda3/envs/lemurdx-data/bin/python"

mkdir -p "$OUTPUT_DIR"

current="$START_DATE"
day_num=0
total_days=$(( ( $(date -d "$END_DATE" +%s) - $(date -d "$START_DATE" +%s) ) / 86400 + 1 ))

echo "Exporting $MEASURE_NAME for device $DEVICE_ID"
echo "Date range: $START_DATE to $END_DATE ($total_days days)"
echo "Output directory: $OUTPUT_DIR"
echo "---"

success=0
fail=0
nodata=0

while [[ "$current" < "$END_DATE" || "$current" == "$END_DATE" ]]; do
    day_num=$((day_num + 1))
    outfile="${OUTPUT_DIR}/${DEVICE_ID}_${MEASURE_NAME}_${current}.csv"

    printf "Day %d/%d: %s ... " "$day_num" "$total_days" "$current"

    if $PYTHON "$SCRIPT_DIR/export.py" \
        --device_id "$DEVICE_ID" \
        --measure_name "$MEASURE_NAME" \
        --start_time "$current 00:00:00" \
        --end_time "$current 23:59:59" \
        --output_file "$outfile" > /dev/null 2>&1; then

        if [[ -f "$outfile" ]]; then
            rows=$(wc -l < "$outfile")
            echo "OK ($((rows - 1)) rows)"
            success=$((success + 1))
        else
            echo "no data"
            nodata=$((nodata + 1))
        fi
    else
        echo "FAILED"
        fail=$((fail + 1))
    fi

    current=$(date -d "$current + 1 day" +%Y-%m-%d)
done

echo "---"
echo "Done: $success exported, $nodata empty, $fail failed"
