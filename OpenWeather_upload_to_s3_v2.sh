#!/bin/bash

# Ensure PATH includes /usr/bin so aws CLI can be found by cron
export PATH=/usr/local/bin:/usr/bin:/bin

## BEGIN DEBUG & LOGGING 🚀
LOGFILE="/home/ubuntu/weather_project/03.Outputs/s3_upload.log"
exec &>> "$LOGFILE"        # Redirect stdout & stderr to log
set -x                      # Echo each command for debugging

echo "========== Script Start: $(date) =========="
echo "Running as user: $(whoami)"
echo "Current directory: $(pwd)"
echo "PATH = $PATH"
echo "Which aws: $(which aws)"
echo "AWS version: $(aws --version || echo 'aws CLI not found')"
echo "Caller identity: $(aws sts get-caller-identity || echo 'No AWS identity')"

## END DEBUG INIT

TODAY=$(date -d "yesterday" +%F) 
SRC_DIR="/home/ubuntu/weather_project/03.Outputs"
BUCKET_NAME="open-data-downloads"
S3_PREFIX="OpenWeatherAPI/openweatherForecast"

FILE2="${SRC_DIR}/${TODAY}_NO_108weatherST_data.parquet"

echo "Checking FILE2: $FILE2"
ls -l "$FILE2" || echo "$FILE2 does not exist."

## Function to try upload
upload_and_cleanup() {
    local file="$1"
    if [[ -f "$file" ]]; then
        echo "Uploading $file to S3..."
        aws s3 cp "$file" s3://${BUCKET_NAME}/${S3_PREFIX}/ --dryrun || true
        aws s3 cp "$file" s3://${BUCKET_NAME}/${S3_PREFIX}/
        local exit_code=$?
        if [[ $exit_code -eq 0 ]]; then
            echo "Successfully uploaded $file"
            rm "$file" && echo "Deleted $file"
        else
            echo "Failed to upload $file, exit code $exit_code"
        fi
    else
        echo "File not found: $file"
    fi
}

## Attempt upload for both files
upload_and_cleanup "$FILE2"

echo "========== Script End: $(date) =========="
