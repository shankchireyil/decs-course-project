#!/bin/bash

# Monitor script for disk I/O during load testing
echo "=== System Monitoring Script ==="
echo "Run this in a separate terminal while load testing"
echo "Press Ctrl+C to stop monitoring"
echo

# Find the main disk device
DISK_DEVICE=$(df / | tail -1 | awk '{print $1}' | sed 's/[0-9]*$//')
echo "Monitoring disk device: $DISK_DEVICE"
echo

# Get MySQL and server PIDs
MYSQL_PID=$(pgrep -f mysqld | head -1)
SERVER_PID=$(pgrep -f kv-server | head -1)

echo "MySQL PID: $MYSQL_PID"
echo "Server PID: $SERVER_PID"
echo "=================================================="

# Monitor loop
while true; do
    echo "$(date '+%H:%M:%S') ---- System Stats ----"
    
    # Disk I/O stats
    iostat -x 1 1 | grep -E "Device|$DISK_DEVICE" | tail -2
    
    # Per-process I/O if PIDs exist
    if [ ! -z "$MYSQL_PID" ]; then
        echo "MySQL I/O:"
        if [ -f "/proc/$MYSQL_PID/io" ]; then
            awk '/^(read_bytes|write_bytes)/ {print "  " $1 ": " $2/1024/1024 " MB"}' /proc/$MYSQL_PID/io
        fi
    fi
    
    if [ ! -z "$SERVER_PID" ]; then
        echo "Server I/O:"
        if [ -f "/proc/$SERVER_PID/io" ]; then
            awk '/^(read_bytes|write_bytes)/ {print "  " $1 ": " $2/1024/1024 " MB"}' /proc/$SERVER_PID/io
        fi
    fi
    
    # CPU wait time
    echo "CPU I/O wait: $(vmstat 1 2 | tail -1 | awk '{print $16}')%"
    
    echo "=================================================="
    sleep 2
done
