#!/usr/bin/env bash
set -euo pipefail

API_BASE="${API_BASE:-http://127.0.0.1:8081}"
SEED_BASE=42
BATCH_SIZE=40
BATCH_INTERVAL=15

# ============================================================
# CẤU HÌNH KỊCH BẢN THỰC NGHIỆM
# ============================================================
SCENARIO="${SCENARIO:-baseline}"

case "$SCENARIO" in
    baseline)
        NUM_WORKERS=20
        RUNS=1
        echo ">>> Chạy kịch bản: Baseline (A1 - A4)"
        ;;
    scalability)
        NUM_WORKERS=20
        RUNS=1
        echo ">>> Chạy kịch bản: Scalability (B1 - B5)"
        ;;
    dynamic)
        NUM_WORKERS=20
        RUNS=1
        echo ">>> Chạy kịch bản: Dynamic Workload (D1 - D4)"
        ;;
    fault)
        NUM_WORKERS=20
        RUNS=1
        echo ">>> Chạy kịch bản: Fault Tolerance (F1 - F5)"
        ;;
    *)
        echo "SCENARIO không hợp lệ! Chọn: baseline | scalability | dynamic | fault"
        exit 1
        ;;
esac

# ============================================================
# DANH SÁCH THUẬT TOÁN
# ============================================================
ALGOS=("PSO_ACO_GA")

declare -A MODE_MAP
MODE_MAP["PSO_ACO_GA"]="PSO_ACO_GA"
MODE_MAP["PSO_ACO_GA_NoPSO"]="PSOACOGA_NOPSO"
MODE_MAP["PSO_ACO_GA_NoGA"]="PSOACOGA_NOGA"
MODE_MAP["EcoTaskOpt"]="ECOTASKOPT"
MODE_MAP["Greedy"]="GREEDY"

mkdir -p results/${SCENARIO}

# ============================================================
# HÀM CHẠY MỘT THỰC NGHIỆM
# ============================================================
run_experiment() {
    local jobs=$1
    local run=$2
    local seed_run=$3

    echo ""
    echo "================================================================"
    echo "RUN $run | Tasks=$jobs | SEED=$seed_run | SCENARIO=$SCENARIO"
    echo "================================================================"

    TASKS_JSON="results/${SCENARIO}/tasks_t${jobs}_run${run}_seed${seed_run}.json"

    # Tạo file tasks nếu chưa tồn tại
    if [ ! -f "$TASKS_JSON" ]; then
        python3 - <<PY
import json, random
rng = random.Random($seed_run)
tasks = []
popular_types = [1, 2, 3]
for i in range($jobs):
    if rng.random() < 0.4:
        task_type = rng.choice(popular_types)
    else:
        task_type = rng.randint(1, 7)
    tasks.append({"type": task_type, "duration": 3, "input_mode": "camera"})
with open("$TASKS_JSON", "w") as f:
    json.dump(tasks, f)
print(f"→ Created {len(tasks)} tasks")
PY
    fi

    for algo in "${ALGOS[@]}"; do
        echo ""
        echo ">>> Running: $algo"

        docker compose down -v --remove-orphans --timeout 25 2>/dev/null || true
        docker rm -f $(docker ps -a -q --filter "name=worker-") 2>/dev/null || true

        SCHEDULER_MODE=${MODE_MAP[$algo]} docker compose up -d --build

        until curl -s "$API_BASE/api/jobs/available" >/dev/null; do sleep 3; done
        sleep 8

        # Gửi tác vụ theo batch
        python3 - <<PY2
import json, time, urllib.request
api = "$API_BASE"
with open("$TASKS_JSON") as f:
    tasks = json.load(f)
submitted = 0
while submitted < len(tasks):
    batch_size = min($BATCH_SIZE, len(tasks) - submitted)
    for _ in range(batch_size):
        if submitted >= len(tasks): break
        task = tasks[submitted]
        urllib.request.urlopen(urllib.request.Request(
            api + "/api/jobs",
            data=json.dumps(task).encode(),
            headers={"Content-Type": "application/json"},
            method="POST"
        ))
        submitted += 1
    if submitted < len(tasks):
        time.sleep($BATCH_INTERVAL)
PY2

        # ============================================================
        # CHỜ ĐẾN KHI HOÀN THÀNH (ĐÃ KHẮC PHỤC TIMEOUT)
        # ============================================================
        MAX_WAIT_SECONDS=${MAX_WAIT_SECONDS:-86400}   # Mặc định chờ tối đa 24 giờ
        POLL_INTERVAL=10
        start_time=$(date +%s)

        echo ""
        echo ">>> Đang chờ tất cả task hoàn thành (tối đa ${MAX_WAIT_SECONDS}s)..."

        while true; do
            rep=$(curl -s "$API_BASE/api/eval/report" 2>/dev/null || echo '{}')
            done_count=$(echo "$rep" | jq '.total_completed + .total_failed + .total_stopped' 2>/dev/null || echo 0)
            failed=$(echo "$rep" | jq '.total_failed' 2>/dev/null || echo 0)

            # In tiến độ
            printf "\r    Tiến độ: %s/%s | Thất bại: %s | Đã chạy: %ss" \
                "$done_count" "$jobs" "$failed" "$(( $(date +%s) - start_time ))"

            if [ "$done_count" -ge "$jobs" ]; then
                echo ""
                echo ">>> ✅ TẤT CẢ $jobs TASK ĐÃ HOÀN THÀNH!"
                break
            fi

            # Kiểm tra giới hạn thời gian an toàn
            elapsed=$(( $(date +%s) - start_time ))
            if [ "$elapsed" -gt "$MAX_WAIT_SECONDS" ]; then
                echo ""
                echo ">>> ⚠️ Đã vượt quá thời gian chờ tối đa. Dừng chờ."
                break
            fi

            sleep $POLL_INTERVAL
        done

        # Lưu kết quả
        OUT_FILE="results/${SCENARIO}/report_${algo}_t${jobs}_run${run}_seed${seed_run}.json"
        curl -s "$API_BASE/api/eval/report" | jq '.' > "$OUT_FILE"
        echo "→ Saved: $OUT_FILE"

        sleep 6
    done
}

# ============================================================
# CHẠY THEO KỊCH BẢN
# ============================================================
if [ "$SCENARIO" = "baseline" ]; then
    # Baseline: A1 - A4
    for jobs in 1000 2000 5000 10000; do
        for run in $(seq 1 $RUNS); do
            seed_run=$((SEED_BASE + run))
            run_experiment $jobs $run $seed_run
        done
    done

else
    # Các kịch bản còn lại
    for run in $(seq 1 $RUNS); do
        seed_run=$((SEED_BASE + run))
        run_experiment 1000 $run $seed_run
    done
fi

echo ""
echo "=================================================="
echo "HOÀN TẤT KỊCH BẢN: $SCENARIO"
echo "Kết quả nằm tại: results/${SCENARIO}/"
echo "=================================================="