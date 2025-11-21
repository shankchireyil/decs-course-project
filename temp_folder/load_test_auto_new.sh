#!/bin/bash

echo "=== Systematic Load Testing Script ==="
echo "Make sure your server is running first!"
echo ""

OUTPUT_FILE="load_test_results.txt"

echo "=== Load Test Results ===" > $OUTPUT_FILE
echo "" >> $OUTPUT_FILE

run_test() {
    local threads=$1
    local duration=$2
    local workload=$3

    echo "Testing workload=$workload | threads=$threads | duration=$duration"

    echo "============================" >> $OUTPUT_FILE
    echo "Test: workload=$workload, threads=$threads, duration=$duration" >> $OUTPUT_FILE
    echo "Timestamp: $(date)" >> $OUTPUT_FILE
    echo "============================" >> $OUTPUT_FILE

    # Run load test & capture its output
    (taskset -c 2-6 ./load_test -t $threads -d $duration -o $workload 2>&1) >> $OUTPUT_FILE &
    LOAD_PID=$!

    # CPU monitor – ONLY cores 0 & 1 (where server is pinned)
    mpstat -P 0,1 1 > mpstat_temp.txt &
    MPSTAT_PID=$!

    # Disk monitor – ALL disks (we will filter after)
    iostat -dx 1 > iostat_temp.txt &
    IOSTAT_PID=$!

    # Wait for load test to complete
    wait $LOAD_PID

    # Stop monitors
    kill $MPSTAT_PID  2>/dev/null
    kill $IOSTAT_PID  2>/dev/null

    ####### CPU UTILISATION (cores 0+1 ONLY) #######
    # (idle is column 13 → usage = 100 - idle)
    AVG_CPU=$(awk '$3 ~ /^[0-9]+$/ && NF>=13 { idle=$13; sum+=100-idle; count++ } END { if(count>0) print sum/count; else print 0 }' mpstat_temp.txt)
    echo "Average CPU Utilisation (cores 0+1): $AVG_CPU %" >> $OUTPUT_FILE

    ####### DISK UTILISATION  (only actual disk, not loop devices) #######
    # Find real disk name automatically (e.g., nvme0n1 OR sda)
    DISK_NAME=$(lsblk -nd --output NAME,TYPE | awk '$2=="disk"{print $1}')

    # Calculate %util ONLY for that disk
    AVG_DISK=$(awk -v disk="$DISK_NAME" '$1 == disk && NF>=14 { sum+=$NF; count++ } END{ if(count>0) print sum/count; else print 0 }' iostat_temp.txt)
    echo "Average Disk Utilisation ($DISK_NAME): $AVG_DISK %" >> $OUTPUT_FILE

    echo -e "\n" >> $OUTPUT_FILE

    rm mpstat_temp.txt iostat_temp.txt 2>/dev/null
}

# RUN TESTS
# for t in 2 4 6 8 10 12 14 16 18 20
for t in 2 5 10 20 30 40 50 60 70 80 90 100
do
    run_test $t 30 "put-all"
done

echo "All tests done. Check: $OUTPUT_FILE"
