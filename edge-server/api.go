package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"cluster-docker/shared"

	"github.com/gorilla/mux"
	"github.com/hashicorp/serf/serf"
	"github.com/sirupsen/logrus"
)

var schedulerScoreRe = regexp.MustCompile(`score=([0-9]+(?:\.[0-9]+)?)`)

type perTaskConfigFile struct {
	Derived struct {
		Complexity           float64 `json:"complexity"`
		CPUReq               float64 `json:"cpu_req"`
		MemReq               float64 `json:"mem_req"`
		EstimatedProcessTime float64 `json:"estimated_process_time"`
		PreTime              float64 `json:"pre_time"`
		PowerFactor          float64 `json:"power_factor"`
	} `json:"derived"`
}

type APIServer struct {
	cluster *ClusterManager
	logger  *logrus.Logger
	router  *mux.Router
}

var _ = (*APIServer).createJob
var _ = (*APIServer).getAvailableJobs
var _ = (*APIServer).claimJob
var _ = (*APIServer).stopJob
var _ = (*APIServer).getEvalReport
var _ = (*APIServer).getMetricsJSON
var _ = (*APIServer).setupAPIRoutes
var _ = (*APIServer).corsMiddleware
var _ = func(api *APIServer) *mux.Router { return api.router }
var _ = func(api *APIServer) *logrus.Logger { return api.logger }

// setupAPIRoutes configures HTTP API routes
func (api *APIServer) setupAPIRoutes() {
	api.router = mux.NewRouter()
	api.router.Use(api.corsMiddleware)
	api.router.HandleFunc("/api/jobs", api.createJob).Methods("POST")
	api.router.HandleFunc("/api/jobs/available", api.getAvailableJobs).Methods("GET")
	api.router.HandleFunc("/api/jobs/{id}/claim", api.claimJob).Methods("POST")
	api.router.HandleFunc("/api/jobs/{id}/stop", api.stopJob).Methods("POST")
	api.router.HandleFunc("/api/eval/report", api.getEvalReport).Methods("GET")
	api.router.HandleFunc("/api/metrics/json", api.getMetricsJSON).Methods("GET")
	// File management endpoints
	api.router.HandleFunc("/api/files", api.listFiles).Methods("GET")
	api.router.HandleFunc("/api/files/{path:.*}", api.serveFile).Methods("GET")
	api.router.HandleFunc("/api/files/upload", api.uploadFile).Methods("POST")
	// Resource serving endpoint for binaries and models
	api.router.HandleFunc("/api/resources/{name}", api.serveResource).Methods("GET")
}

func (api *APIServer) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// createJob handles job creation via HTTP POST
func (api *APIServer) createJob(w http.ResponseWriter, r *http.Request) {
	var req shared.JobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.Type < 1 || req.Type > 10 {
		http.Error(w, "Invalid task type", http.StatusBadRequest)
		return
	}

	if req.Duration <= 0 {
		http.Error(w, "Invalid duration", http.StatusBadRequest)
		return
	}
	//// Durations 1–9 are likely mistakes (e.g. task type used as duration); use 30s
	//if req.Duration < 10 {
	//	req.Duration = 30
	//}

	// Default to camera mode if not specified
	inputMode := req.InputMode
	if inputMode == "" {
		inputMode = shared.InputModeCamera
	}

	filePath := req.FilePath
	fileType := req.FileType
	fileURL := req.FileURL
	if inputMode == shared.InputModeFile && fileURL != "" {
		if newPath, newType, newURL, err := api.ensureEdgeHostedFile(fileURL, filePath, fileType); err != nil {
			http.Error(w, fmt.Sprintf("Failed to fetch file URL on edge: %v", err), http.StatusBadRequest)
			return
		} else {
			filePath, fileType, fileURL = newPath, newType, newURL
		}
	}

	// Optional per-job config JSON (hosted on edge)
	cfgPath := ""
	cfgURL := ""
	complexity := 0.0
	cpuReq := 0.0
	memReq := 0.0
	estProc := 0.0
	preTime := 0.0
	powerFactor := 0.0
	if strings.TrimSpace(req.ConfigURL) != "" {
		if newPath, newURL, err := api.ensureEdgeHostedConfig(req.ConfigURL, req.ConfigPath); err != nil {
			http.Error(w, fmt.Sprintf("Failed to fetch config URL on edge: %v", err), http.StatusBadRequest)
			return
		} else {
			cfgPath, cfgURL = newPath, newURL
			// Parse derived fields from the JSON file on disk.
			fileDir := os.Getenv("FILE_STORAGE_DIR")
			if fileDir == "" {
				fileDir = "./files"
			}
			full := filepath.Join(fileDir, filepath.Clean(cfgPath))
			if b, err := os.ReadFile(full); err == nil {
				var cfg perTaskConfigFile
				if err := json.Unmarshal(b, &cfg); err == nil {
					complexity = cfg.Derived.Complexity
					cpuReq = cfg.Derived.CPUReq
					memReq = cfg.Derived.MemReq
					estProc = cfg.Derived.EstimatedProcessTime
					preTime = cfg.Derived.PreTime
					powerFactor = cfg.Derived.PowerFactor
				}
			}
		}
	}

	taskID := api.cluster.addJob(
		req.Type,
		req.Duration,
		inputMode,
		filePath,
		fileType,
		fileURL,
		cfgPath,
		cfgURL,
		complexity,
		cpuReq,
		memReq,
		estProc,
		preTime,
		powerFactor,
	)

	// Do not call broadcastClusterState while holding cluster.mutex (it needs to read cluster state).
	// Run asynchronously so the handler can return and release the lock first.
	go api.cluster.edgeServer.broadcastClusterState()

	if api.cluster.edgeServer.clusterMQTT != nil {
		api.cluster.edgeServer.clusterMQTT.republishAllPendingJobs(api.cluster)
	}

	response := shared.JobResponse{
		TaskID:   taskID,
		Assigned: false,
		Message:  "Job created and queued",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// ensureEdgeHostedConfig downloads an external JSON config URL onto the edge server and returns:
// - configPath: relative path inside FILE_STORAGE_DIR (e.g. "cfg_xxx.json")
// - configURL: relative URL for workers/clients to download from edge (e.g. "/api/files/cfg_xxx.json")
func (api *APIServer) ensureEdgeHostedConfig(rawURL, filePathHint string) (string, string, error) {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return "", "", fmt.Errorf("invalid url: %w", err)
	}

	// If it's already an edge-served relative URL, keep as-is.
	if u.Host == "" && strings.HasPrefix(u.Path, "/api/files/") {
		p := strings.TrimPrefix(u.Path, "/api/files/")
		p = strings.TrimPrefix(p, "/")
		return p, u.Path, nil
	}

	// If it's already pointing to edge /api/files, keep as-is (clients will resolve via EDGE_API_BASE).
	if u.Host != "" && strings.Contains(u.Path, "/api/files/") {
		idx := strings.Index(u.Path, "/api/files/")
		rel := u.Path[idx:]
		p := strings.TrimPrefix(rel, "/api/files/")
		p = strings.TrimPrefix(p, "/")
		return p, rel, nil
	}

	fileDir := os.Getenv("FILE_STORAGE_DIR")
	if fileDir == "" {
		fileDir = "./files"
	}
	if err := os.MkdirAll(fileDir, 0o755); err != nil {
		return "", "", fmt.Errorf("failed to create FILE_STORAGE_DIR: %w", err)
	}

	base := strings.TrimSpace(filePathHint)
	if base == "" {
		base = filepath.Base(u.Path)
	}
	if base == "" || base == "/" || base == "." {
		base = "config.json"
	}
	// Ensure .json extension
	if strings.ToLower(filepath.Ext(base)) != ".json" {
		base = base + ".json"
	}
	// Sanitize
	base = strings.ReplaceAll(base, "?", "_")
	base = strings.ReplaceAll(base, "&", "_")
	base = strings.ReplaceAll(base, "=", "_")
	base = strings.ReplaceAll(base, " ", "_")
	dstName := fmt.Sprintf("cfg_%d_%s", time.Now().UnixNano()%100000000, base)
	dstPath := filepath.Join(fileDir, dstName)

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Get(rawURL)
	if err != nil {
		return "", "", fmt.Errorf("download failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("download failed: status %d", resp.StatusCode)
	}

	out, err := os.Create(dstPath)
	if err != nil {
		return "", "", fmt.Errorf("failed to create config file: %w", err)
	}
	defer out.Close()
	if _, err := io.Copy(out, resp.Body); err != nil {
		return "", "", fmt.Errorf("failed to save config file: %w", err)
	}

	return dstName, "/api/files/" + dstName, nil
}

// ensureEdgeHostedFile downloads an external file URL onto the edge server (MilkV may have no internet),
// optionally converts MP4 -> H264, and returns:
// - filePath: relative path inside FILE_STORAGE_DIR (e.g. "dl_xxx.h264")
// - fileType: "h264" or "mp4"
// - fileURL: relative URL for workers to download from edge (e.g. "/api/files/dl_xxx.h264")
func (api *APIServer) ensureEdgeHostedFile(rawURL, filePathHint, fileTypeHint string) (string, string, string, error) {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return "", "", "", fmt.Errorf("invalid url: %w", err)
	}

	// If it's already an edge-served relative URL, keep as-is.
	if u.Host == "" && strings.HasPrefix(u.Path, "/api/files/") {
		p := strings.TrimPrefix(u.Path, "/api/files/")
		p = strings.TrimPrefix(p, "/")
		return p, fileTypeHint, u.Path, nil
	}

	// If it's already pointing to edge /api/files, keep as-is (workers will resolve via EDGE_API_BASE).
	if u.Host != "" && strings.Contains(u.Path, "/api/files/") {
		// Use relative URL so MilkV never tries localhost.
		idx := strings.Index(u.Path, "/api/files/")
		rel := u.Path[idx:]
		p := strings.TrimPrefix(rel, "/api/files/")
		p = strings.TrimPrefix(p, "/")
		return p, fileTypeHint, rel, nil
	}

	fileDir := os.Getenv("FILE_STORAGE_DIR")
	if fileDir == "" {
		fileDir = "./files"
	}
	if err := os.MkdirAll(fileDir, 0o755); err != nil {
		return "", "", "", fmt.Errorf("failed to create FILE_STORAGE_DIR: %w", err)
	}

	// Derive a filename
	base := filepath.Base(u.Path)
	if base == "" || base == "/" || base == "." {
		base = "download"
	}
	// Simple sanitize
	base = strings.ReplaceAll(base, "?", "_")
	base = strings.ReplaceAll(base, "&", "_")
	base = strings.ReplaceAll(base, "=", "_")

	ext := strings.ToLower(filepath.Ext(base))
	ft := strings.ToLower(strings.TrimSpace(fileTypeHint))
	if ft == "" {
		if ext == ".mp4" {
			ft = "mp4"
		} else if ext == ".h264" {
			ft = "h264"
		}
	}

	// Unique prefix to avoid collisions
	prefix := fmt.Sprintf("dl_%d_", time.Now().UnixNano())
	dstName := prefix + base
	dstPath := filepath.Join(fileDir, dstName)

	api.logger.Infof("Fetching external file on edge: %s -> %s", rawURL, dstPath)
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Get(rawURL)
	if err != nil {
		return "", "", "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", "", "", fmt.Errorf("download failed: status %d", resp.StatusCode)
	}

	out, err := os.Create(dstPath)
	if err != nil {
		return "", "", "", fmt.Errorf("failed to create file: %w", err)
	}
	_, copyErr := io.Copy(out, resp.Body)
	closeErr := out.Close()
	if copyErr != nil {
		return "", "", "", fmt.Errorf("failed to save file: %w", copyErr)
	}
	if closeErr != nil {
		return "", "", "", fmt.Errorf("failed to close file: %w", closeErr)
	}

	// If mp4, try to convert to raw h264 for MilkV
	if ft == "mp4" || ext == ".mp4" {
		h264Name := strings.TrimSuffix(dstName, filepath.Ext(dstName)) + ".h264"
		h264Path := filepath.Join(fileDir, h264Name)
		api.logger.Infof("Converting MP4 to H264 on edge: %s -> %s", dstPath, h264Path)
		cmd := exec.Command("ffmpeg", "-y", "-i", dstPath, "-c:v", "libx264", "-an", "-f", "h264", h264Path)
		if err := cmd.Run(); err != nil {
			api.logger.Warnf("ffmpeg conversion failed on edge (keeping MP4): %v", err)
			// Keep mp4
			rel := dstName
			return rel, "mp4", "/api/files/" + rel, nil
		}
		_ = os.Remove(dstPath)
		return h264Name, "h264", "/api/files/" + h264Name, nil
	}

	// Default: treat as h264 if extension says so
	if ft == "" && ext == ".h264" {
		ft = "h264"
	}
	if ft == "" {
		ft = "h264"
	}

	return dstName, ft, "/api/files/" + dstName, nil
}

// getAvailableJobs returns a list of available (pending) jobs
func (api *APIServer) getAvailableJobs(w http.ResponseWriter, r *http.Request) {
	api.cluster.mutex.RLock()
	availableJobs := make([]shared.Task, 0)
	for _, job := range api.cluster.jobQueue {
		if job.Status == shared.TaskStatusPending {
			availableJobs = append(availableJobs, job)
		}
	}
	api.cluster.mutex.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(availableJobs)
}

// claimJob handles job claiming by worker nodes
func (api *APIServer) claimJob(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobID := vars["id"]

	var claimReq struct {
		NodeID string `json:"node_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&claimReq); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Read Serf members without holding the cluster lock (avoids lock inversion deadlocks).
	var members []serf.Member
	if api.cluster != nil && api.cluster.edgeServer != nil && api.cluster.edgeServer.serf != nil {
		members = api.cluster.edgeServer.serf.Members()
	}

	var claimingMember *serf.Member
	for _, m := range members {
		if m.Name == claimReq.NodeID && m.Status == serf.StatusAlive {
			mCopy := m
			claimingMember = &mCopy
			break
		}
	}
	if claimingMember == nil {
		http.Error(w, "Node not found or not alive", http.StatusNotFound)
		return
	}

	status, hasStatus := claimingMember.Tags["status"]
	// Only reject if status is explicitly "busy", allow if missing/empty (treat as "online")
	if hasStatus && status == "busy" {
		http.Error(w, "Node is busy", http.StatusConflict)
		return
	}

	// Only hold cluster.mutex for in-memory state checks/updates.
	api.cluster.mutex.Lock()

	var job *shared.Task
	jobIdx := -1
	for i := range api.cluster.jobQueue {
		if api.cluster.jobQueue[i].ID == jobID && api.cluster.jobQueue[i].Status == shared.TaskStatusPending {
			job = &api.cluster.jobQueue[i]
			jobIdx = i
			break
		}
	}
	if job == nil {
		api.cluster.mutex.Unlock()
		http.Error(w, "Job not found or already claimed", http.StatusNotFound)
		return
	}

	// Check memory availability (with fallback to memory_limit if free_memory not set)
	freeMemoryStr, hasFreeMemory := claimingMember.Tags["free_memory"]
	memoryLimitStr, hasMemoryLimit := claimingMember.Tags["memory_limit"]
	var freeMemory int
	if hasFreeMemory {
		if mem, err := strconv.Atoi(freeMemoryStr); err == nil {
			freeMemory = mem
		}
	} else if hasMemoryLimit {
		// Fallback: use memory_limit as available memory
		if mem, err := strconv.Atoi(memoryLimitStr); err == nil {
			freeMemory = mem
		}
	}
	if freeMemory > 0 && freeMemory < job.MemoryUsage {
		api.cluster.mutex.Unlock()
		http.Error(w, "Node has insufficient memory", http.StatusConflict)
		return
	}

	best := (*serf.Member)(nil)
	bestReason := ""
	if api.cluster.scheduler != nil {
		best, bestReason = api.cluster.scheduler.BestWorkerForTaskLocked(*job, members)
	}
	if best != nil && claimReq.NodeID != best.Name {
		api.cluster.mutex.Unlock()
		http.Error(w, "Job assigned to another node", http.StatusConflict)
		return
	}

	jobCopy := *job
	assignedAt := time.Now()
	schedulerScore := 0.0
	if bestReason != "" {
		if m := schedulerScoreRe.FindStringSubmatch(bestReason); len(m) == 2 {
			if v, err := strconv.ParseFloat(m[1], 64); err == nil {
				schedulerScore = v
			}
		}
	}
	// Remove from queue immediately on assignment so it doesn't stay in "waiting" forever,
	// even if the worker fails before sending a "started" status.
	if jobIdx >= 0 {
		api.cluster.jobQueue = append(api.cluster.jobQueue[:jobIdx], api.cluster.jobQueue[jobIdx+1:]...)
	}
	if api.cluster.scheduler != nil {
		api.cluster.scheduler.OnAssigned(claimReq.NodeID, jobCopy)
	}
	if api.cluster.jobHistory != nil {
		if rec := api.cluster.jobHistory[jobCopy.ID]; rec != nil {
			rec.AssignedTo = claimReq.NodeID
			rec.AssignedAt = assignedAt
			rec.Status = shared.TaskStatusRunning
			rec.SchedulerScore = schedulerScore
			rec.Complexity = jobCopy.Complexity
			rec.DataSizeBits = jobCopy.DataSizeBits
			// Processing power is fixed per worker (see metrics.fixedProcessingPowerGHz)
			rec.NodeSpeed = ProcessingPowerGHzForWorker(claimReq.NodeID)
		}
	}

	api.cluster.mutex.Unlock()

	if api.cluster.metrics != nil {
		api.cluster.metrics.OnTaskAssigned(claimReq.NodeID, schedulerScore)
	}

	if api.cluster.edgeServer.clusterMQTT != nil {
		api.cluster.edgeServer.clusterMQTT.publishAssignment(claimReq.NodeID, jobCopy)
	}

	if bestReason != "" {
		api.cluster.logger.Infof("Job %s assigned to %s via HTTP (scheduler: %s)", jobID, claimReq.NodeID, bestReason)
	} else {
		api.cluster.logger.Infof("Job %s assigned to %s via HTTP", jobID, claimReq.NodeID)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(&jobCopy)

	go api.cluster.edgeServer.broadcastClusterState()
	go func() {
		time.Sleep(300 * time.Millisecond)
		api.cluster.edgeServer.broadcastClusterState()
	}()
}

// stopJob stops a queued job (removes it from the queue) or cancels a running job (signals the worker).
func (api *APIServer) stopJob(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobID := vars["id"]
	if jobID == "" {
		http.Error(w, "Missing job id", http.StatusBadRequest)
		return
	}

	type resp struct {
		TaskID  string `json:"task_id"`
		Status  string `json:"status"`
		Message string `json:"message"`
		NodeID  string `json:"node_id,omitempty"`
	}

	// 1) Try to remove from queued (pending) jobs under lock.
	removed := false
	api.cluster.mutex.Lock()
	for i := range api.cluster.jobQueue {
		if api.cluster.jobQueue[i].ID == jobID && api.cluster.jobQueue[i].Status == shared.TaskStatusPending {
			api.cluster.jobQueue = append(api.cluster.jobQueue[:i], api.cluster.jobQueue[i+1:]...)
			removed = true
			break
		}
	}
	if removed && api.cluster.jobHistory != nil {
		if rec := api.cluster.jobHistory[jobID]; rec != nil {
			rec.Status = shared.TaskStatusStopped
			rec.CompletedAt = time.Now()
		}
	}
	api.cluster.mutex.Unlock()

	if removed {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp{
			TaskID:  jobID,
			Status:  "stopped",
			Message: "Job removed from queue",
		})
		go api.cluster.edgeServer.broadcastClusterState()
		return
	}

	// 2) If it's running, signal the node via Serf user event (broadcast; node filters by node_id).
	if api.cluster == nil || api.cluster.edgeServer == nil || api.cluster.edgeServer.serf == nil {
		http.Error(w, "Cluster not ready", http.StatusServiceUnavailable)
		return
	}

	members := api.cluster.edgeServer.serf.Members()
	runningNode := ""
	for _, m := range members {
		if m.Status != serf.StatusAlive || m.Name == "edge-server" {
			continue
		}
		if m.Tags["current_task_id"] == jobID {
			runningNode = m.Name
			break
		}
	}
	if runningNode == "" {
		http.Error(w, "Job not found (not queued or running)", http.StatusNotFound)
		return
	}

	payload := map[string]interface{}{
		"node_id": runningNode,
		"task_id": jobID,
	}
	data, _ := json.Marshal(payload)
	_ = api.cluster.edgeServer.serf.UserEvent("task-cancel", data, false)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp{
		TaskID:  jobID,
		Status:  "cancelling",
		Message: "Cancel signal sent to worker",
		NodeID:  runningNode,
	})
	go api.cluster.edgeServer.broadcastClusterState()
}

func (api *APIServer) getEvalReport(w http.ResponseWriter, r *http.Request) {
	mode := "PSO_ACO_GA"
	if api.cluster != nil && api.cluster.scheduler != nil {
		mode = api.cluster.scheduler.Mode()
	} else if s := strings.TrimSpace(os.Getenv("SCHEDULER_MODE")); s != "" {
		mode = s
	}

	now := time.Now()

	api.cluster.mutex.RLock()
	records := make([]*JobRecord, 0, len(api.cluster.jobHistory))
	for _, rec := range api.cluster.jobHistory {
		// Copy pointer targets shallowly (safe: we never mutate these outside lock).
		records = append(records, rec)
	}
	api.cluster.mutex.RUnlock()

	report := EvalReport{
		SchedulerMode:     mode,
		AssignmentsByNode: map[string]int{},
	}

	var (
		firstCreate time.Time
		lastDone    time.Time
		latencies   []float64
	)

	for _, rec := range records {
		report.TotalCreated++
		if firstCreate.IsZero() || rec.CreatedAt.Before(firstCreate) {
			firstCreate = rec.CreatedAt
		}
		if rec.AssignedTo != "" {
			report.AssignmentsByNode[rec.AssignedTo]++
		}
		switch rec.Status {
		case shared.TaskStatusCompleted:
			report.TotalCompleted++
			if rec.DurationSec > 0 {
				latencies = append(latencies, rec.DurationSec)
			}
			if lastDone.IsZero() || rec.CompletedAt.After(lastDone) {
				lastDone = rec.CompletedAt
			}
		case shared.TaskStatusFailed:
			report.TotalFailed++
			if lastDone.IsZero() || rec.CompletedAt.After(lastDone) {
				lastDone = rec.CompletedAt
			}
		case shared.TaskStatusStopped:
			report.TotalStopped++
			if lastDone.IsZero() || rec.CompletedAt.After(lastDone) {
				lastDone = rec.CompletedAt
			}
		default:
			report.TotalPending++
		}
	}

	if !firstCreate.IsZero() && !lastDone.IsZero() && lastDone.After(firstCreate) {
		report.MakespanSec = lastDone.Sub(firstCreate).Seconds()
	}
	// Throughput: completed jobs per wallclock since first create
	if !firstCreate.IsZero() {
		elapsed := now.Sub(firstCreate).Seconds()
		if elapsed > 0 {
			report.ThroughputJobsPerSec = float64(report.TotalCompleted) / elapsed
		}
	}

	if len(latencies) > 0 {
		sum := 0.0
		for _, v := range latencies {
			sum += v
		}
		report.LatencyMeanSec = sum / float64(len(latencies))
		sort.Float64s(latencies)
		report.LatencyP50Sec = percentile(latencies, 0.50)
		report.LatencyP95Sec = percentile(latencies, 0.95)
	}

	// Fairness (Gini) across nodes: assignments
	if len(report.AssignmentsByNode) > 0 {
		vals := make([]float64, 0, len(report.AssignmentsByNode))
		for _, v := range report.AssignmentsByNode {
			vals = append(vals, float64(v))
		}
		report.GiniAssignments = gini(vals)
	}

	// Per-worker totals for Gini (energy and duration)
	workerEnergy := map[string]float64{}
	workerDuration := map[string]float64{}
	const energyJPerGc = 0.9
	for _, rec := range records {
		if rec.DurationSec <= 0 {
			continue
		}
		if rec.Status != shared.TaskStatusCompleted && rec.Status != shared.TaskStatusFailed && rec.Status != shared.TaskStatusStopped {
			continue
		}
		w := rec.AssignedTo
		if w == "" {
			continue
		}
		powerGHz := ProcessingPowerGHzForWorker(w)
		gc := rec.DurationSec * powerGHz
		workerEnergy[w] += energyJPerGc * gc
		workerDuration[w] += rec.DurationSec
	}
	if len(workerEnergy) > 0 {
		vals := make([]float64, 0, len(workerEnergy))
		for _, v := range workerEnergy {
			vals = append(vals, v)
		}
		report.GiniEnergy = gini(vals)
	}
	if len(workerDuration) > 0 {
		vals := make([]float64, 0, len(workerDuration))
		for _, v := range workerDuration {
			vals = append(vals, v)
		}
		report.GiniDuration = gini(vals)
	}

	// Processing power (Gc/s), energy (0.9 J/Gc), computation intensity (cycles/bit)
	report.EnergyJPerGc = energyJPerGc
	var totalGigacycles, totalDurationSec float64
	var sumCyclesPerBit float64
	var nIntensity int
	for _, rec := range records {
		if rec.DurationSec <= 0 {
			continue
		}
		if rec.Status != shared.TaskStatusCompleted && rec.Status != shared.TaskStatusFailed && rec.Status != shared.TaskStatusStopped {
			continue
		}
		powerGHz := ProcessingPowerGHzForWorker(rec.AssignedTo)
		gc := rec.DurationSec * powerGHz
		totalGigacycles += gc
		totalDurationSec += rec.DurationSec
		bits := rec.DataSizeBits
		if bits <= 0 && rec.MemoryUsage > 0 {
			bits = float64(rec.MemoryUsage) * 8 * 1024 * 1024
		}
		if bits > 0 {
			sumCyclesPerBit += (gc * 1e9) / bits
			nIntensity++
		}
	}
	report.TotalEnergyJoules = energyJPerGc * totalGigacycles
	if totalDurationSec > 0 {
		report.ProcessingPowerGcPerSec = totalGigacycles / totalDurationSec
	}
	if nIntensity > 0 {
		report.AvgComputationIntensityCPB = sumCyclesPerBit / float64(nIntensity)
	}

	// ----- batching evaluation -----
	tasksPerWorker, batchingCounts, totalSavings := calculateBatchingSavings(records)
	report.TasksPerWorker = tasksPerWorker
	report.BatchingCountPerWorker = batchingCounts
	report.TotalBatchingSavings = totalSavings

	// === DECISION LATENCY (từ scheduler) ===
	avgDecisionMs := 0.0
	totalDecisionSec := 0.0
	if api.cluster != nil && api.cluster.scheduler != nil && len(api.cluster.scheduler.decisionLatencies) > 0 {
		sum := 0.0
		for _, lat := range api.cluster.scheduler.decisionLatencies {
			sum += lat
		}
		avgDecisionMs = sum / float64(len(api.cluster.scheduler.decisionLatencies))
		totalDecisionSec = sum / 1000.0
	}
	report.AvgDecisionLatencyMs = avgDecisionMs
	report.TotalDecisionLatencySec = totalDecisionSec

	// === CONVERGENCE SPEED (đã sửa cho api.go) ===
	convergenceSpeed := 0.0
	overallConvergenceSpeed := 0.0
	avgConvergenceRate := 0.0

	// 1. Recent convergence (dựa trên 20 tasks gần nhất - cho phép âm)
	if api.cluster != nil && api.cluster.scheduler != nil && len(api.cluster.scheduler.recentPerf) >= 2 {
		recentPerf := api.cluster.scheduler.recentPerf
		initial := recentPerf[0]
		final := recentPerf[len(recentPerf)-1]
		convergenceSpeed = (initial - final) / float64(len(recentPerf))

		// Average Convergence Rate (ACR)
		sumRate := 0.0
		valid := 0
		for i := 1; i < len(recentPerf); i++ {
			if recentPerf[i-1] > 0 {
				rate := (recentPerf[i-1] - recentPerf[i]) / recentPerf[i-1]
				sumRate += rate
				valid++
			}
		}
		if valid > 0 {
			avgConvergenceRate = sumRate / float64(valid)
		}
	}

	// 2. Overall convergence (toàn bộ history - ổn định cho final report)
	if len(records) >= 2 {
		var latencies []float64
		for _, rec := range records {
			if rec.Status == shared.TaskStatusCompleted && rec.DurationSec > 0 {
				latencies = append(latencies, rec.DurationSec)
			}
		}
		if len(latencies) >= 2 {
			initial := latencies[0]
			final := latencies[len(latencies)-1]
			overallConvergenceSpeed = (initial - final) / float64(len(latencies))
		}
	}

	report.ConvergenceSpeed = convergenceSpeed
	report.OverallConvergenceSpeed = overallConvergenceSpeed
	report.AvgConvergenceRate = avgConvergenceRate

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(report)
}

// getMetricsJSON returns a snapshot compatible with PSO-ACO-GA_perTask.go's metrics.json.
func (api *APIServer) getMetricsJSON(w http.ResponseWriter, r *http.Request) {
	if api.cluster == nil || api.cluster.metrics == nil {
		http.Error(w, "Metrics not available", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(api.cluster.metrics.SnapshotJSON())
}

// FileInfo represents file metadata
type FileInfo struct {
	Name     string    `json:"name"`
	Path     string    `json:"path"`
	Size     int64     `json:"size"`
	Type     string    `json:"type"` // "mp4" or "h264"
	Modified time.Time `json:"modified"`
}

// listFiles returns a list of available files (videos + optional configs)
func (api *APIServer) listFiles(w http.ResponseWriter, r *http.Request) {
	// Default file directory - can be configured via env var
	fileDir := os.Getenv("FILE_STORAGE_DIR")
	if fileDir == "" {
		fileDir = "./files"
	}

	// Ensure directory exists
	if err := os.MkdirAll(fileDir, 0755); err != nil {
		http.Error(w, fmt.Sprintf("Failed to access file directory: %v", err), http.StatusInternalServerError)
		return
	}

	var files []FileInfo
	err := filepath.Walk(fileDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip files we can't access
		}
		if info.IsDir() {
			return nil // Skip directories
		}

		ext := strings.ToLower(filepath.Ext(path))
		if ext != ".mp4" && ext != ".h264" && ext != ".json" {
			return nil // Only include known file types
		}

		relPath, err := filepath.Rel(fileDir, path)
		if err != nil {
			relPath = path
		}

		files = append(files, FileInfo{
			Name:     info.Name(),
			Path:     relPath,
			Size:     info.Size(),
			Type:     strings.TrimPrefix(ext, "."),
			Modified: info.ModTime(),
		})
		return nil
	})

	if err != nil {
		api.logger.Warnf("Error walking file directory: %v", err)
		// Return empty array instead of error if directory walk fails
		files = []FileInfo{}
	}

	// Always return a valid JSON array, even if empty
	w.Header().Set("Content-Type", "application/json")
	if files == nil {
		files = []FileInfo{}
	}
	json.NewEncoder(w).Encode(files)
}

// serveFile serves a file for download
func (api *APIServer) serveFile(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	filePath := vars["path"]

	// Default file directory
	fileDir := os.Getenv("FILE_STORAGE_DIR")
	if fileDir == "" {
		fileDir = "./files"
	}

	// Sanitize path to prevent directory traversal
	fullPath := filepath.Join(fileDir, filepath.Clean(filePath))
	if !strings.HasPrefix(fullPath, filepath.Clean(fileDir)+string(os.PathSeparator)) {
		http.Error(w, "Invalid file path", http.StatusBadRequest)
		return
	}

	// Check if file exists
	info, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "File not found", http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf("Failed to access file: %v", err), http.StatusInternalServerError)
		return
	}

	if info.IsDir() {
		http.Error(w, "Path is a directory", http.StatusBadRequest)
		return
	}

	// Open file
	file, err := os.Open(fullPath)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to open file: %v", err), http.StatusInternalServerError)
		return
	}
	defer file.Close()

	// Set headers
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filepath.Base(fullPath)))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", info.Size()))

	// Stream file
	io.Copy(w, file)
}

// uploadFile handles file uploads
func (api *APIServer) uploadFile(w http.ResponseWriter, r *http.Request) {
	// Parse multipart form (10MB max)
	err := r.ParseMultipartForm(10 << 20)
	if err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}

	file, handler, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "No file provided", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Validate file type
	ext := strings.ToLower(filepath.Ext(handler.Filename))
	if ext != ".mp4" && ext != ".h264" {
		http.Error(w, "Only .mp4 and .h264 files are allowed", http.StatusBadRequest)
		return
	}

	// Default file directory
	fileDir := os.Getenv("FILE_STORAGE_DIR")
	if fileDir == "" {
		fileDir = "./files"
	}

	// Ensure directory exists
	if err := os.MkdirAll(fileDir, 0755); err != nil {
		http.Error(w, fmt.Sprintf("Failed to create file directory: %v", err), http.StatusInternalServerError)
		return
	}

	// Create destination file
	fullPath := filepath.Join(fileDir, handler.Filename)
	dst, err := os.Create(fullPath)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to create file: %v", err), http.StatusInternalServerError)
		return
	}
	defer dst.Close()

	// Copy uploaded file
	_, err = io.Copy(dst, file)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to save file: %v", err), http.StatusInternalServerError)
		return
	}

	// Return file info
	info, _ := os.Stat(fullPath)
	fileInfo := FileInfo{
		Name:     handler.Filename,
		Path:     handler.Filename,
		Size:     info.Size(),
		Type:     strings.TrimPrefix(ext, "."),
		Modified: info.ModTime(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(fileInfo)
}

// serveResource serves binary and model files from the resource directory
func (api *APIServer) serveResource(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	resourceName := vars["name"]

	// Default resource directory
	resourceDir := os.Getenv("RESOURCE_DIR")
	if resourceDir == "" {
		resourceDir = "/home/qht/cluster-docker/resource"
	}

	// Sanitize resource name to prevent directory traversal
	if strings.Contains(resourceName, "..") || strings.Contains(resourceName, "/") {
		http.Error(w, "Invalid resource name", http.StatusBadRequest)
		return
	}

	fullPath := filepath.Join(resourceDir, resourceName)

	// Check if file exists
	info, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "Resource not found", http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf("Failed to access resource: %v", err), http.StatusInternalServerError)
		return
	}

	if info.IsDir() {
		http.Error(w, "Path is a directory", http.StatusBadRequest)
		return
	}

	// Open file
	file, err := os.Open(fullPath)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to open file: %v", err), http.StatusInternalServerError)
		return
	}
	defer file.Close()

	// Set appropriate content type
	ext := strings.ToLower(filepath.Ext(resourceName))
	contentType := "application/octet-stream"
	if ext == ".cvimodel" {
		contentType = "application/octet-stream"
	}

	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", resourceName))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", info.Size()))

	// Stream file
	io.Copy(w, file)
}
