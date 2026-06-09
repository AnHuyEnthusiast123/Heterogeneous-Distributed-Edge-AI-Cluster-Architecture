package main

import (
	"math"
	"sort"
	"time"

	"cluster-docker/shared"
)

type JobRecord struct {
	ID          string           `json:"id"`
	Type        shared.TaskType  `json:"type"`
	Name        string           `json:"name"`
	MemoryUsage int              `json:"memory_usage"`
	Duration    int              `json:"duration"`
	InputMode   shared.InputMode `json:"input_mode"`

	CreatedAt time.Time `json:"created_at"`

	AssignedTo string    `json:"assigned_to,omitempty"`
	AssignedAt time.Time `json:"assigned_at,omitempty"`

	CompletedAt time.Time         `json:"completed_at,omitempty"`
	Status      shared.TaskStatus `json:"status"`
	DurationSec float64           `json:"duration_sec,omitempty"`

	SchedulerScore float64 `json:"scheduler_score,omitempty"`

	Complexity   float64 `json:"complexity,omitempty"`
	NodeSpeed    float64 `json:"node_speed,omitempty"`
	DataSizeBits float64 `json:"data_size_bits,omitempty"`
}

type EvalReport struct {
	SchedulerMode string `json:"scheduler_mode"`

	TotalCreated   int `json:"total_created"`
	TotalCompleted int `json:"total_completed"`
	TotalFailed    int `json:"total_failed"`
	TotalStopped   int `json:"total_stopped"`
	TotalPending   int `json:"total_pending"`

	ThroughputJobsPerSec float64 `json:"throughput_jobs_per_sec"`
	MakespanSec          float64 `json:"makespan_sec"`

	LatencyMeanSec float64 `json:"latency_mean_sec"`
	LatencyP50Sec  float64 `json:"latency_p50_sec"`
	LatencyP95Sec  float64 `json:"latency_p95_sec"`

	AssignmentsByNode map[string]int `json:"assignments_by_node"`
	GiniAssignments   float64        `json:"gini_assignments"`
	GiniEnergy        float64        `json:"gini_energy"`
	GiniDuration      float64        `json:"gini_duration"`

	ProcessingPowerGcPerSec    float64 `json:"processing_power_gc_per_sec"`
	TotalEnergyJoules          float64 `json:"total_energy_joules"`
	EnergyJPerGc               float64 `json:"energy_j_per_gc"`
	AvgComputationIntensityCPB float64 `json:"avg_computation_intensity_cycles_per_bit"`

	// Modified: Ordered list IDs per worker (sorted by AssignedAt)
	TasksPerWorker map[string][]string `json:"tasks_per_worker"`
	// New/Modified: Map worker → total pairs (linear consecutive)
	BatchingCountPerWorker map[string]int `json:"batching_count_per_worker"`
	// Modified: Sum savings sec (random 0.3-0.5 per pair)
	TotalBatchingSavings float64 `json:"total_batching_savings"`

	AvgDecisionLatencyMs    float64 `json:"avg_decision_latency_ms"`
	TotalDecisionLatencySec float64 `json:"total_decision_latency_sec"`

	ConvergenceSpeed        float64 `json:"convergence_speed"`    // improvement per task
	AvgConvergenceRate      float64 `json:"avg_convergence_rate"` // ACR theo literature
	OverallConvergenceSpeed float64 `json:"overall_convergence_speed"`
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[len(sorted)-1]
	}
	pos := p * float64(len(sorted)-1)
	i := int(math.Floor(pos))
	j := int(math.Ceil(pos))
	if i == j {
		return sorted[i]
	}
	frac := pos - float64(i)
	return sorted[i]*(1-frac) + sorted[j]*frac
}

func gini(vals []float64) float64 {
	n := len(vals)
	if n == 0 {
		return 0
	}
	sort.Float64s(vals)
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	if sum == 0 {
		return 0
	}
	num := 0.0
	for i, v := range vals {
		num += float64(i+1) * v
	}
	return (2*num)/(float64(n)*sum) - (float64(n)+1)/float64(n)
}

// Modified: calculateBatchingSavings để count consecutive pairs, add ordered tasks_per_worker, random savings per pair
func calculateBatchingSavings(history []*JobRecord) (map[string][]string, map[string]int, float64) {
	tasksPerWorker := make(map[string][]string)
	sortedPerWorker := make(map[string][]*JobRecord) // Temp để sort

	// Build per worker list, only Completed
	for _, rec := range history {
		if rec.Status == shared.TaskStatusCompleted && rec.AssignedTo != "" {
			sortedPerWorker[rec.AssignedTo] = append(sortedPerWorker[rec.AssignedTo], rec)
		}
	}

	batchingCounts := make(map[string]int)
	totalSavings := 0.0

	for worker, recs := range sortedPerWorker {
		// Sort by AssignedAt (exact assign order), fallback CompletedAt
		sort.Slice(recs, func(i, j int) bool {
			if !recs[i].AssignedAt.IsZero() && !recs[j].AssignedAt.IsZero() {
				return recs[i].AssignedAt.Before(recs[j].AssignedAt)
			}
			return recs[i].CompletedAt.Before(recs[j].CompletedAt)
		})

		// Build ordered IDs for tasks_per_worker
		orderedIDs := make([]string, len(recs))
		for i, rec := range recs {
			name, _ := shared.GetTaskInfo(rec.Type)
			orderedIDs[i] = rec.ID + "-" + name
		}
		tasksPerWorker[worker] = orderedIDs

		// Count consecutive pairs
		if len(recs) < 2 {
			continue
		}
		prevType := recs[0].Type
		for i := 1; i < len(recs); i++ {
			currType := recs[i].Type
			if currType == prevType && currType != 0 { // Valid type
				batchingCounts[worker]++
				reduction := 0.4 // 0.3-0.5s per pair
				totalSavings += reduction
			}
			prevType = currType
		}
	}

	return tasksPerWorker, batchingCounts, totalSavings
}

// Modified: getEvalReport để dùng modified calculateBatchingSavings, update fields
func getEvalReport(cm *ClusterManager, schedulerMode string) EvalReport {
	history := make([]*JobRecord, 0, len(cm.jobHistory))
	for _, rec := range cm.jobHistory {
		history = append(history, rec)
	}

	// Modified: Gọi updated func
	tasksPerWorker, batchingCounts, totalSavings := calculateBatchingSavings(history)

	totalCreated := len(history)
	totalCompleted := 0
	totalFailed := 0
	totalStopped := 0
	totalPending := 0
	latencies := make([]float64, 0)
	assignments := make(map[string]int)
	workerDurations := make(map[string]float64)
	workerEnergies := make(map[string]float64)
	minCreated := time.Time{}
	maxCompleted := time.Time{}
	totalGigacycles := 0.0

	for _, rec := range history {
		switch rec.Status {
		case shared.TaskStatusCompleted:
			totalCompleted++
			latency := rec.CompletedAt.Sub(rec.CreatedAt).Seconds()
			latencies = append(latencies, latency)
			assignments[rec.AssignedTo]++
			gc := rec.DurationSec * rec.NodeSpeed
			totalGigacycles += gc
			workerDurations[rec.AssignedTo] += rec.DurationSec
			workerEnergies[rec.AssignedTo] += 0.9 * gc
			if maxCompleted.IsZero() || rec.CompletedAt.After(maxCompleted) {
				maxCompleted = rec.CompletedAt
			}
		case shared.TaskStatusFailed:
			totalFailed++
		case shared.TaskStatusStopped:
			totalStopped++
		case shared.TaskStatusPending:
			totalPending++
		}
		if minCreated.IsZero() || rec.CreatedAt.Before(minCreated) {
			minCreated = rec.CreatedAt
		}
	}

	sort.Float64s(latencies)
	var makespan float64
	if !maxCompleted.IsZero() && !minCreated.IsZero() {
		makespan = maxCompleted.Sub(minCreated).Seconds()
	}
	var throughput float64
	if makespan > 0 {
		throughput = float64(totalCompleted) / makespan
	}
	latencyMean := 0.0
	if totalCompleted > 0 {
		for _, lat := range latencies {
			latencyMean += lat
		}
		latencyMean /= float64(totalCompleted)
	}

	assignVals := make([]float64, 0, len(assignments))
	durVals := make([]float64, 0, len(workerDurations))
	energyVals := make([]float64, 0, len(workerEnergies))
	for _, v := range assignments {
		assignVals = append(assignVals, float64(v))
	}
	for _, v := range workerDurations {
		durVals = append(durVals, v)
	}
	for _, v := range workerEnergies {
		energyVals = append(energyVals, v)
	}

	var processingPower float64
	if makespan > 0 {
		processingPower = totalGigacycles / makespan
	}

	report := EvalReport{
		SchedulerMode:              schedulerMode,
		TotalCreated:               totalCreated,
		TotalCompleted:             totalCompleted,
		TotalFailed:                totalFailed,
		TotalStopped:               totalStopped,
		TotalPending:               totalPending,
		ThroughputJobsPerSec:       throughput,
		MakespanSec:                makespan,
		LatencyMeanSec:             latencyMean,
		LatencyP50Sec:              percentile(latencies, 0.5),
		LatencyP95Sec:              percentile(latencies, 0.95),
		AssignmentsByNode:          assignments,
		GiniAssignments:            gini(assignVals),
		GiniDuration:               gini(durVals),
		GiniEnergy:                 gini(energyVals),
		ProcessingPowerGcPerSec:    processingPower,
		TotalEnergyJoules:          0.9 * totalGigacycles,
		EnergyJPerGc:               0.9,
		AvgComputationIntensityCPB: 0.0, // Placeholder

		// Modified: Ordered by assign time
		TasksPerWorker: tasksPerWorker,
		// Modified: Per worker pairs
		BatchingCountPerWorker: batchingCounts,
		// Modified: Sum random savings
		TotalBatchingSavings: totalSavings,
	}

	avgDecisionMs := 0.0
	totalDecisionSec := 0.0
	if cm.scheduler != nil && len(cm.scheduler.decisionLatencies) > 0 {
		sum := 0.0
		for _, lat := range cm.scheduler.decisionLatencies {
			sum += lat
		}
		avgDecisionMs = sum / float64(len(cm.scheduler.decisionLatencies))
		totalDecisionSec = sum / 1000.0
	}
	report.AvgDecisionLatencyMs = avgDecisionMs
	report.TotalDecisionLatencySec = totalDecisionSec

	// === CONVERGENCE SPEED (chuẩn khoa học) ===
	convergenceSpeed := 0.0
	overallConvergenceSpeed := 0.0
	avgConvergenceRate := 0.0

	// 1. Recent convergence (20 tasks cuối - cho phép âm)
	if len(cm.scheduler.recentPerf) >= 2 {
		initial := cm.scheduler.recentPerf[0]
		final := cm.scheduler.recentPerf[len(cm.scheduler.recentPerf)-1]
		convergenceSpeed = (initial - final) / float64(len(cm.scheduler.recentPerf))

		// ACR (Average Convergence Rate)
		sumRate := 0.0
		valid := 0
		for i := 1; i < len(cm.scheduler.recentPerf); i++ {
			if cm.scheduler.recentPerf[i-1] > 0 {
				rate := (cm.scheduler.recentPerf[i-1] - cm.scheduler.recentPerf[i]) / cm.scheduler.recentPerf[i-1]
				sumRate += rate
				valid++
			}
		}
		if valid > 0 {
			avgConvergenceRate = sumRate / float64(valid)
		}
	}

	for _, rec := range history {
		if rec.Status == shared.TaskStatusCompleted && rec.DurationSec > 0 {
			latencies = append(latencies, rec.DurationSec)
		}
	}
	if len(latencies) >= 2 {
		initial := latencies[0]
		final := latencies[len(latencies)-1]
		overallConvergenceSpeed = (initial - final) / float64(len(latencies))
	}

	report.ConvergenceSpeed = convergenceSpeed
	report.OverallConvergenceSpeed = overallConvergenceSpeed
	report.AvgConvergenceRate = avgConvergenceRate

	return report
}
