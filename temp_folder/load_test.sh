#!/bin/bash

echo "=== Systematic Load Testing Script ==="
echo "Make sure your server is running first!"
echo ""

# Function to run test and show results
run_test() {
    local threads=$1
    local duration=$2
    local workload=$3
    local desc=$4
    
    echo "Testing: $desc"
    echo "Command: ./kv-client-civetweb -t $threads -d $duration -o $workload"
    echo "Monitor server CPU usage during this test..."
    echo "Press Enter to continue or Ctrl+C to stop"
    read
    
    ./kv-client-civetweb -t $threads -d $duration -o $workload
    echo ""
    echo "=== Check server CPU usage now ==="
    echo "If server CPU < 50%, increase threads"
    echo "If server CPU > 95%, reduce threads"
    echo "Press Enter for next test..."
    read
    echo ""
}

# Test sequence
run_test 50 15 "get-popular" "Light Cache Load (50 threads)"
run_test 100 15 "get-popular" "Medium Cache Load (100 threads)" 
run_test 200 15 "get-popular" "Heavy Cache Load (200 threads)"
run_test 300 15 "get-popular" "Very Heavy Cache Load (300 threads)"

echo "Now testing disk-bound workloads..."
run_test 100 15 "put-all" "Disk Write Load (100 threads)"
run_test 150 15 "get-all" "Disk Read Load (150 threads)"

echo "Mixed workload tests..."
run_test 200 30 "get-put" "Mixed Load (200 threads)"

echo "Load testing complete!"
echo ""
echo "Based on the results, use the thread count that achieves:"
echo "- 80-90% server CPU for sustained load testing"
echo "- 95-100% server CPU for maximum stress testing"
