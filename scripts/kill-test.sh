#!/bin/bash

# Exit on any failure
set -e

echo ""
echo "=== EtchFlow Kill Test ==="
echo ""

# Generate a single run ID for both the script and the verification
RUN_ID="killtest-$(date +%s)-$RANDOM"
echo ">>> Starting 5-node graph (nodes sleep 2s each)..."
echo ">>> Using RUN_ID: $RUN_ID"

# Start the python script in the background
export PYTHONPATH="$PYTHONPATH:$(pwd)/etchflow-py-sdk"
python examples/langgraph_demo.py --run-id "$RUN_ID" &
PYTHON_PID=$!

echo ">>> Graph running (PID=$PYTHON_PID)"
echo ">>> Waiting 8s (nodes 1-3 complete, node 4 mid-execution)..."
sleep 8

echo ""
echo ">>> Killing Python process..."
kill -9 $PYTHON_PID 2>/dev/null || true
sleep 1

echo ">>> Verifying EtchFlow is still alive..."
curl -sf http://localhost:8080/health > /dev/null && echo "✅ EtchFlow alive"

echo ""
echo ">>> Resuming run (Python will auto-resume since the ID matches)..."
export PYTHONPATH="$PYTHONPATH:$(pwd)/etchflow-py-sdk"
python examples/langgraph_demo.py --run-id "$RUN_ID"

echo ""
echo "=== Kill Test Complete ==="
