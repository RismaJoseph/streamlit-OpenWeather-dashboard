#!/bin/bash

# Ensure PATH includes aws CLI
export PATH=/usr/local/bin:/usr/bin:/bin

LOGFILE="/home/ubuntu/weather_project/03.Outputs/duckdb_backup.log"
exec &>> "$LOGFILE"
set -x

echo "========== Backup Start: $(date) =========="

DUCKDB_PATH="/home/ubuntu/weather_project/03.Outputs/NO_openWeatherData_API.duckdb"
BUCKET_NAME="open-data-downloads"
S3_PREFIX="OpenWeatherAPI/openweatherForecast"

# Timestamp for backup filename, e.g., 2025-06-28
BACKUP_DATE=$(date +%F)

# Backup filename with date
BACKUP_FILE="NO_openWeatherData_API_${BACKUP_DATE}.duckdb"

if [[ -f "$DUCKDB_PATH" ]]; then
    echo "Uploading $DUCKDB_PATH to s3://${BUCKET_NAME}/${S3_PREFIX}/${BACKUP_FILE}..."
    aws s3 cp "$DUCKDB_PATH" "s3://${BUCKET_NAME}/${S3_PREFIX}/${BACKUP_FILE}"
    if [[ $? -eq 0 ]]; then
        echo "Backup uploaded successfully"
    else
        echo "Backup upload failed"
    fi
else
    echo "DuckDB file not found: $DUCKDB_PATH"
fi

echo "========== Backup End: $(date) =========="
