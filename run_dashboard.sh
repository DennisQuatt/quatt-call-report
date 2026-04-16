#!/bin/bash
# Only run Mon–Fri 07:00–21:00 Amsterdam time
# Skip if a previous run is still active (e.g. during full history fetch)
LOCK=/tmp/aircall_dashboard.lock

if [ -f "$LOCK" ]; then
    PID=$(cat "$LOCK" 2>/dev/null)
    if kill -0 "$PID" 2>/dev/null; then
        echo "[$(date)] Skipping — run $PID still active" >> /tmp/aircall_cron.log
        exit 0
    fi
fi

DOW=$(TZ='Europe/Amsterdam' date +%u)   # 1=Mon … 7=Sun
HOUR=$(TZ='Europe/Amsterdam' date +%H)
MIN=$(TZ='Europe/Amsterdam' date +%M)
TIME=$((10#$HOUR * 100 + 10#$MIN))

if [ "$DOW" -ge 1 ] && [ "$DOW" -le 5 ] && [ "$TIME" -ge 700 ] && [ "$TIME" -le 2100 ]; then
    echo $$ > "$LOCK"
    cd '/Users/dennisquatt/aircall-dashboard' && \
    PYTHONUNBUFFERED=1 /usr/bin/python3 update_dashboard.py >> /tmp/aircall_cron.log 2>&1
    rm -f "$LOCK"
fi
