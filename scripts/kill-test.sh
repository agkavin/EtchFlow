#!/bin/bash
# Kill Test Script - Tests EtchFlow crash recovery

set -e

echo "=== EtchFlow Kill Test ==="
echo ""

# Clean start - clear DB and file
docker compose exec -T postgres psql -U etchflow -c "TRUNCATE runs CASCADE;" 2>/dev/null || true
rm -f examples/.run_id

# Step 1: Start fresh run and kill it
echo "[1] Starting demo in background..."
nohup python examples/demo.py > /tmp/demo.log 2>&1 &
PID=$!
echo "    PID: $PID"

# Wait for run_id file (process must create it before checkpoint)
echo "    Waiting for run_id file..."
for i in {1..30}; do
    if [ -f examples/.run_id ]; then
        break
    fi
    sleep 1
done

if [ ! -f examples/.run_id ]; then
    echo "    ERROR: run_id file not created!"
    kill -9 $PID 2>/dev/null || true
    cat /tmp/demo.log | tail -20
    exit 1
fi

RUN_ID=$(cat examples/.run_id)
echo "    Run ID: $RUN_ID"
echo "    Waiting 15 seconds (nodes take 10s each)..."
sleep 15

echo "    Killing process..."
kill -9 $PID 2>/dev/null || true
wait $PID 2>/dev/null || true

# Step 2: Check checkpoint
echo ""
echo "[2] Last checkpoint:"
LAST=$(curl -s http://localhost:8080/runs/$RUN_ID | python -c "import sys,json; print(json.load(sys.stdin).get('last_node_completed','none'))")
echo "    Last node: $LAST"

# Step 3: Resume
echo ""
echo "[3] Resuming..."
python examples/demo.py --resume > /tmp/resume.log 2>&1

# Step 4: Verify
echo ""
echo "[4] Final:"
STATUS=$(curl -s http://localhost:8080/runs/$RUN_ID | python -c "import sys,json; print(json.load(sys.stdin).get('status','?'))")
echo "    Status: $STATUS"

if [ "$STATUS" = "SUCCESS" ]; then
    echo ""
    echo "=== ✅ KILL TEST PASSED ==="
else
    echo ""
    echo "=== ❌ KILL TEST FAILED ==="
    exit 1
fi