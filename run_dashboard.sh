#!/bin/bash
# Only run Mon–Fri 07:00–21:00 Amsterdam time
DOW=$(TZ='Europe/Amsterdam' date +%u)   # 1=Mon … 7=Sun
HOUR=$(TZ='Europe/Amsterdam' date +%H)
MIN=$(TZ='Europe/Amsterdam' date +%M)
TIME=$((10#$HOUR * 100 + 10#$MIN))

if [ "$DOW" -ge 1 ] && [ "$DOW" -le 5 ] && [ "$TIME" -ge 700 ] && [ "$TIME" -le 2100 ]; then
    cd '/Users/dennisquatt/aircall-dashboard' && \
    PYTHONUNBUFFERED=1 /usr/bin/python3 update_dashboard.py >> /tmp/aircall_cron.log 2>&1
fi
