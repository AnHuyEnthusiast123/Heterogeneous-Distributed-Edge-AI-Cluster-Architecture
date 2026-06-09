package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"cluster-docker/shared"
	"github.com/sirupsen/logrus"
)

/*
Bo sung vao 19:04 24/3/2026
Tinh toan lai Energy
*/
const (
	energyAlpha          = 1.0  // J per Million Instructions
	energyBeta           = 5e-9 // J per bit (transmission + setup)
	energyJPerGcFallback = 0.9  // fallback nếu không có MI
)

func MIPSForWorker(nodeID string) float64 {
	ghz := ProcessingPowerGHzForWorker(nodeID)
	return ghz * 1000.0
}

// EdgeMetrics keeps metric names compatible with PSO-ACO-GA_perTask.go so results are on the same scale.
type EdgeMetrics struct {
	logger  *logrus.Logger
	cluster *ClusterManager

	startTime time.Time

	startOnce sync.Once
}

func NewEdgeMetrics(logger *logrus.Logger, cluster *ClusterManager) *EdgeMetrics {
	return &EdgeMetrics{
		logger:    logger,
		cluster:   cluster,
		startTime: time.Now(),
	}
}

// Start launches:
// - periodic metrics.json export (and in-memory snapshot for API)
func (m *EdgeMetrics) Start() {
	m.startOnce.Do(func() {
		go func() {
			t := time.NewTicker(30 * time.Second)
			defer t.Stop()
			for range t.C {
				m.exportMetricsJSON("metrics.json")
			}
		}()
	})
}

// Processing power is fixed per worker (0.5–1.0 GHz). Matches docker-compose NODE_SPEED.
const (
	processingPowerGHzMin     = 0.5
	processingPowerGHzMax     = 1.0
	processingPowerGHzDefault = 0.75
)

// fixedProcessingPowerGHz maps normalized worker ID to fixed processing power in GHz.
// Used for all energy and computation-intensity metrics; keep in sync with docker-compose NODE_SPEED.
// Power descends A→E (A strongest, E weakest).
var fixedProcessingPowerGHz = map[string]float64{
	"A": 1.00,
	"B": 1.00,
	"C": 1.00,
	"D": 1.00,
	"E": 1.00,
	"F": 1.00,
	"G": 1.00,
	"H": 1.00,
	"I": 1.00,
	"J": 1.00,
	"K": 1.00,
	"L": 1.00,
	"M": 1.00,
	"N": 1.00,
	"O": 1.00,
}

// ProcessingPowerGHzForWorker returns the fixed processing power in GHz for a worker (by node ID).
// Unknown workers get processingPowerGHzDefault.
func ProcessingPowerGHzForWorker(nodeID string) float64 {
	w := normalizeWorkerLabel(nodeID)
	if w == "" {
		return processingPowerGHzDefault
	}
	if ghz, ok := fixedProcessingPowerGHz[w]; ok {
		return ghz
	}
	return processingPowerGHzDefault
}

// processingPowerGHz returns the worker processing power in GHz, clamped to [0.5, 1.0].
// Used when only a numeric speed is available (e.g. from tags); prefer ProcessingPowerGHzForWorker when node ID is known.
func processingPowerGHz(speed float64) float64 {
	if speed <= 0 {
		return processingPowerGHzDefault
	}
	if speed < processingPowerGHzMin {
		return processingPowerGHzMin
	}
	if speed > processingPowerGHzMax {
		return processingPowerGHzMax
	}
	return speed
}

func normalizeWorkerLabel(nodeID string) string {
	nodeID = strings.TrimSpace(nodeID)
	if strings.HasPrefix(nodeID, "milkv-") && len(nodeID) > len("milkv-") {
		return nodeID[len("milkv-"):]
	}
	return nodeID
}

func (m *EdgeMetrics) OnTaskAssigned(nodeID string, schedulerScore float64) {
	_ = schedulerScore // stored in jobHistory; exported via JSON snapshot
}

func (m *EdgeMetrics) OnTaskCompletion(nodeID string, status shared.TaskStatus, durationSec float64) {
	_, _, _ = nodeID, status, durationSec
}

func simulatedDurationSecForTaskType(tt shared.TaskType) float64 {
	switch tt {
	case shared.TaskDrowsiness: // 1
		return 1.82
	case shared.TaskHeatmap: // 2
		return 3.59
	case shared.TaskFaceDetection: // 3
		return 2.53
	case shared.TaskPeopleCounting: // 4
		return 1.56 //1.56
	case shared.TaskObjectTracking: // 5
		return 1.34
	case shared.TaskLicensePlateRecognition: // 6
		return 1.76
	case shared.TaskGestureRecognition: // 7
		return 3.2
	case shared.TaskAnomalyDetection: // 8
		return 1.89
	case shared.TaskVehicleCounting: // 9
		return 2.73
	case shared.TaskCrowdDensityAnalysis: // 10
		return 4.56
	default:
		return 1.0
	}
}

// SnapshotJSON returns a JSON-serializable map compatible with the original metrics.json export.
func (m *EdgeMetrics) SnapshotJSON() map[string]any {
	// Copy job history + queue under lock.
	var records []JobRecord
	pending := 0
	if m.cluster != nil {
		m.cluster.mutex.RLock()
		for _, j := range m.cluster.jobQueue {
			if j.Status == shared.TaskStatusPending {
				pending++
			}
		}
		records = make([]JobRecord, 0, len(m.cluster.jobHistory))
		for _, rec := range m.cluster.jobHistory {
			if rec != nil {
				records = append(records, *rec)
			}
		}
		m.cluster.mutex.RUnlock()
	}

	type agg struct {
		assigned int
		errs     int
		sumDur   float64
		nDur     int
		sumScore float64
		nScore   int
		sumGc    float64
	}
	byWorker := map[string]*agg{}
	getAgg := func(w string) *agg {
		a := byWorker[w]
		if a == nil {
			a = &agg{}
			byWorker[w] = a
		}
		return a
	}

	totalCompleted := 0
	totalLatency := 0.0
	totalLatencyN := 0
	var totalGigacycles, totalDurationSec float64

	for _, rec := range records {
		w := normalizeWorkerLabel(rec.AssignedTo)
		if w != "" && w != "edge-server" {
			getAgg(w).assigned++
		}
		if rec.Status == shared.TaskStatusFailed && w != "" {
			getAgg(w).errs++
		}
		if rec.DurationSec > 0 && (rec.Status == shared.TaskStatusCompleted || rec.Status == shared.TaskStatusFailed || rec.Status == shared.TaskStatusStopped) {
			getAgg(w).sumDur += rec.DurationSec
			getAgg(w).nDur++
			totalLatency += rec.DurationSec
			totalLatencyN++
			powerGHz := ProcessingPowerGHzForWorker(rec.AssignedTo)
			gc := rec.DurationSec * powerGHz
			totalGigacycles += gc
			totalDurationSec += rec.DurationSec
			if w != "" {
				getAgg(w).sumGc += gc
			}
		}
		if rec.SchedulerScore > 0 && (rec.Status == shared.TaskStatusCompleted || rec.Status == shared.TaskStatusFailed || rec.Status == shared.TaskStatusStopped) {
			getAgg(w).sumScore += rec.SchedulerScore
			getAgg(w).nScore++
		}
		if rec.Status == shared.TaskStatusCompleted {
			totalCompleted++
		}
	}

	// worker_load (0/1) from Serf membership tags if available.
	loads := map[string]float64{}
	if m.cluster != nil && m.cluster.edgeServer != nil && m.cluster.edgeServer.serf != nil {
		for _, member := range m.cluster.edgeServer.serf.Members() {
			if member.Name == "edge-server" {
				continue
			}
			w := normalizeWorkerLabel(member.Name)
			if w == "" {
				continue
			}
			if member.Tags["status"] == "busy" {
				loads[w] = 1.0
			} else {
				loads[w] = 0.0
			}
		}
	}

	// load_variance + bias_ratio (same formula style as original)
	loadVals := make([]float64, 0, len(loads))
	minLoad := math.MaxFloat64
	maxLoad := 0.0
	for _, v := range loads {
		loadVals = append(loadVals, v)
		if v < minLoad {
			minLoad = v
		}
		if v > maxLoad {
			maxLoad = v
		}
	}
	avgLoad := 0.0
	for _, v := range loadVals {
		avgLoad += v
	}
	if len(loadVals) > 0 {
		avgLoad /= float64(len(loadVals))
	}
	var sumSq float64
	for _, v := range loadVals {
		sumSq += math.Pow(v-avgLoad, 2)
	}
	variance := 0.0
	if len(loadVals) > 0 {
		variance = sumSq / float64(len(loadVals))
	}
	biasRatio := 0.0
	if minLoad > 0 && minLoad < math.MaxFloat64 {
		biasRatio = maxLoad / minLoad
	}

	avgLatency := 0.0
	if totalLatencyN > 0 {
		avgLatency = totalLatency / float64(totalLatencyN)
	}

	avgAffinityScore := 0.0
	scoreSum := 0.0
	scoreN := 0
	for _, a := range byWorker {
		if a.nScore > 0 {
			scoreSum += a.sumScore
			scoreN += a.nScore
		}
	}
	if scoreN > 0 {
		avgAffinityScore = scoreSum / float64(scoreN)
	}

	elapsed := time.Since(m.startTime).Seconds()
	throughput := 0.0
	if elapsed > 0 {
		throughput = float64(totalCompleted) / elapsed
	}

	// Processing power (Gc/s), energy (0.9 J/Gc), computation intensity (cycles/bit)
	/*
		const energyJPerGc = 0.9
		totalEnergyJoules := energyJPerGc * totalGigacycles
		processingPowerGcPerSec := 0.0
		if totalDurationSec > 0 {
			processingPowerGcPerSec = totalGigacycles / totalDurationSec
		}
		avgComputationIntensityCyclesPerBit := 0.0
		if nIntensity > 0 {
			avgComputationIntensityCyclesPerBit = sumCyclesPerBit / float64(nIntensity)
		}*/
	// Tinh theo cong thuc EMAPSO
	totalEnergyJoules := 0.0
	avgComputationIntensityCyclesPerBit := 0.0
	nIntensity := 0
	sumCyclesPerBit := 0.0

	for _, rec := range records {
		if rec.DurationSec <= 0 {
			continue
		}

		// exeTimeSec = base time (tại speed=1.0) / speed hiện tại
		speed := ProcessingPowerGHzForWorker(rec.AssignedTo)
		baseSec := simulatedDurationSecForTaskType(rec.Type) * float64(rec.Duration)
		exeTimeSec := baseSec / speed

		// S_n = dataSizeBits (Sn-dataSize)
		bits := rec.DataSizeBits
		if bits <= 0 {
			if rec.InputMode == shared.InputModeFile && rec.MemoryUsage > 0 {
				// File-based task: estimate from MemoryUsage (≈ file size in MB)
				bits = float64(rec.MemoryUsage) * 8 * 1024 * 1024
			} else {
				bits = 1024.0 * 8 // ~1 KB RPC metadata for camera tasks
			}
		}

		// Energy theo paper
		energy := energyAlpha*exeTimeSec + energyBeta*bits
		totalEnergyJoules += energy

		// Computation intensity (cycles/bit)
		if bits > 0 {
			gc := rec.DurationSec * ProcessingPowerGHzForWorker(rec.AssignedTo)
			cycles := gc * 1e9
			sumCyclesPerBit += cycles / bits
			nIntensity++
		}
	}

	if nIntensity > 0 {
		avgComputationIntensityCyclesPerBit = sumCyclesPerBit / float64(nIntensity)
	}

	processingPowerGcPerSec := 0.0
	if totalDurationSec > 0 {
		processingPowerGcPerSec = totalGigacycles / totalDurationSec
	}

	out := map[string]any{
		"queue_length":                float64(pending),
		"load_variance":               variance,
		"avg_latency":                 avgLatency,
		"avg_affinity_score":          avgAffinityScore,
		"bias_ratio":                  biasRatio,
		"throughput":                  throughput,
		"processing_power_gc_per_sec": processingPowerGcPerSec,
		"total_energy_joules":         totalEnergyJoules,
		"total_gigacycles":            totalGigacycles,
		"avg_computation_intensity_cycles_per_bit": avgComputationIntensityCyclesPerBit,
	}

	// Per-worker keys (giữ nguyên)
	workers := make([]string, 0, len(byWorker))
	for w := range byWorker {
		workers = append(workers, w)
	}
	sort.Strings(workers)
	for _, w := range workers {
		a := byWorker[w]
		out[fmt.Sprintf("tasks_assigned_%s", w)] = float64(a.assigned)
		avgPT := 0.0
		if a.nDur > 0 {
			avgPT = a.sumDur / float64(a.nDur)
		}
		out[fmt.Sprintf("avg_process_time_%s", w)] = avgPT
		out[fmt.Sprintf("task_errors_%s", w)] = float64(a.errs)
		out[fmt.Sprintf("worker_load_%s", w)] = loads[w]
		out[fmt.Sprintf("total_energy_joules_%s", w)] = 0.0 // có thể tính chi tiết nếu cần
	}

	return out
}

func (m *EdgeMetrics) exportMetricsJSON(path string) {
	data := m.SnapshotJSON()
	b, err := json.MarshalIndent(data, "", " ")
	if err != nil {
		return
	}
	_ = os.WriteFile(path, b, 0o644)
}
