#!/bin/bash

# === CONFIGURATION ===
start_date="2025-06-28"  # set to today's date
max_days=30
script_path="/home/ubuntu/weather_project/02.Codes/01.Openweather_APIdata_fetch.py"
log_file="/home/ubuntu/weather_project/03.Outputs/APIweatherDataFetch.log"

# === LOGIC ===
today=$(date +%Y-%m-%d)
days_elapsed=$(( ( $(date -d "$today" +%s) - $(date -d "$start_date" +%s) ) / 86400 ))

if [ "$days_elapsed" -lt "$max_days" ]; then
    echo "[$(date)] Running weather script (Day $((days_elapsed+1)) of $max_days)" >> $log_file
    /usr/bin/python3 $script_path >> $log_file 2>&1
else
    echo "[$(date)] 30 days completed. No more execution." >> $log_file
fi
