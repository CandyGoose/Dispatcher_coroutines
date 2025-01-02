#!/bin/bash

PROGRAM="./build/bin/app"

DURATION=15

LOG_DIR="monitor_logs"
TOP_LOG="$LOG_DIR/top.log"
VMSTAT_LOG="$LOG_DIR/vmstat.log"

mkdir -p $LOG_DIR

> "$TOP_LOG"
> "$VMSTAT_LOG"

echo "Запускаем программу $PROGRAM..."
$PROGRAM &
PID=$!

echo "Программа запущена с PID=$PID. Сбор метрик в течение $DURATION секунд..."

echo "Сбор метрик top..."
top -b -d 1 -n $DURATION -p $PID > "$TOP_LOG" &

echo "Сбор метрик vmstat..."
vmstat 1 $DURATION > "$VMSTAT_LOG" &

wait $PID
echo "Программа завершена."

wait
echo "Сбор метрик завершён. Логи сохранены в директорию $LOG_DIR."

echo "Итоги мониторинга:"
echo "  - top: $TOP_LOG"
echo "  - vmstat: $VMSTAT_LOG"
