#!/bin/bash

echo "=== Systematic Load Testing Script ==="
echo "Make sure your server is running first!"
echo ""

OUTPUT_FILE="load_test_results.txt"
echo "=== Load Test Results ===" > "$OUTPUT_FILE"
echo "" >> "$OUTPUT_FILE"

run_test() {
    local threads=$1
    local duration=$2
    local workload=$3

    echo "Testing workload=$workload | threads=$threads | duration=$duration"

    echo "============================" >> "$OUTPUT_FILE"
    echo "Test: workload=$workload, threads=$threads, duration=$duration" >> "$OUTPUT_FILE"
    echo "Timestamp: $(date)" >> "$OUTPUT_FILE"
    echo "============================" >> "$OUTPUT_FILE"

    # Run load test & capture its output
    (taskset -c 2-8 ./load_test -t "$threads" -d "$duration" -o "$workload" 2>&1) >> "$OUTPUT_FILE" &
    LOAD_PID=$!

    # CPU monitor – cores 0 & 1 (server pinned here)
    mpstat -P 0,1 1 > mpstat_temp.txt &
    MPSTAT_PID=$!

    # Disk monitor – for later parsing
    iostat -dx 1 > iostat_temp.txt &
    IOSTAT_PID=$!

    # Wait for load test to finish
    wait $LOAD_PID

    # Stop monitors
    kill $MPSTAT_PID  2>/dev/null
    kill $IOSTAT_PID  2>/dev/null

    ############ CPU UTILISATION (cores 0+1) ############
    AVG_CPU=$(awk '$3 ~ /^[0-9]+$/ && NF>=13 { idle=$13; sum+=100-idle; count++ } 
                   END { if(count>0) print sum/count; else print 0 }' mpstat_temp.txt)
    echo "Average CPU Utilisation (cores 0+1): $AVG_CPU %" >> "$OUTPUT_FILE"

    ############ DISK UTILISATION & THROUGHPUT ############
    # Find actual disk (not loop devices)
    DISK_NAME=$(lsblk -nd --output NAME,TYPE | awk '$2=="disk"{print $1}' | head -n 1)

    echo "Detected Disk: $DISK_NAME" >> "$OUTPUT_FILE"

    # Get column number of wkB/s (auto-detect)
    WKBS_COL=$(awk '/^Device/ { for(i=1;i<=NF;i++) if($i=="wkB/s") print i; exit }' iostat_temp.txt)

    # Average wkB/s for that disk
    AVG_WKBS=$(awk -v disk="$DISK_NAME" -v col="$WKBS_COL" ' 
        $1==disk && $col ~ /^[0-9.]+$/ { sum += $col; count++ } 
        END { if(count>0) print sum/count; else print 0 }
    ' iostat_temp.txt)
    echo "Average Write KB/s ($DISK_NAME): $AVG_WKBS" >> "$OUTPUT_FILE"

    # Average %util for that disk
    AVG_DISK_UTIL=$(awk -v disk="$DISK_NAME" '$1 == disk && NF>=14 { sum+=$NF; count++ } 
                     END{ if(count>0) print sum/count; else print 0 }' iostat_temp.txt)
    echo "Average Disk Utilisation ($DISK_NAME): $AVG_DISK_UTIL %" >> "$OUTPUT_FILE"

    echo -e "\n" >> "$OUTPUT_FILE"

    rm mpstat_temp.txt iostat_temp.txt 2>/dev/null
}

############ RUN TESTS ############
# for t in 2 4 6 8 10 12 14 16 18 20
# for t in  2 4 6 8 10 12 14 16 18 20
for t in 2 4 6 8 10 12 14 16 18 20
do
    run_test "$t" 30 "get-popular"
done

echo "All tests done. Check: $OUTPUT_FILE"
