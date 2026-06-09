package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"cluster-docker/shared"

	"github.com/grandcat/zeroconf"
	"github.com/hashicorp/serf/serf"
	"github.com/sirupsen/logrus"
)

type MilkvNode struct {
	serf        *serf.Serf
	nodeID      string
	memoryLimit int

	// currentTask is accessed by multiple goroutines (scheduler, metrics loop, cancel handler).
	// Protect pointer reads/writes to avoid races and nil dereferences.
	taskMu      sync.RWMutex
	currentTask *shared.Task
	logger      *logrus.Logger
	serfJoin    string
	edgeAPIBase string
	mdnsServer  *zeroconf.Server
	useMQTT     bool
	nodeMQTT    *NodeMQTT
	taskWorkDir string
	nodeIP      string

	// Throttle MQTT metrics so we don't build up large in-memory publish queues on slow links/brokers.
	lastMQTTMetricsNS int64

	// Serf events (used for task cancellation)
	eventCh chan serf.Event

	// Current task cancellation
	cancelMu          sync.Mutex
	currentCancel     context.CancelFunc
	currentCancelTask string
	lastType          shared.TaskType
}

func (m *MilkvNode) getCurrentTask() *shared.Task {
	m.taskMu.RLock()
	t := m.currentTask
	m.taskMu.RUnlock()
	return t
}

func (m *MilkvNode) setCurrentTask(t *shared.Task) {
	m.taskMu.Lock()
	m.currentTask = t
	m.taskMu.Unlock()
}

// getStableNodeID returns a stable node ID, preferring:
// 1) NODE_ID env var (if set)
// 2) eMMC CID from /sys/block/mmcblkN/device/cid (persistent storage ID)
// 3) MAC address of the first non-loopback, UP interface
// 4) Random suffix fallback
func getStableNodeID(logger *logrus.Logger) string {
	// 1) Explicit override
	if envID := os.Getenv("NODE_ID"); envID != "" {
		return envID
	}

	// 2) Try eMMC CID from common mmcblk devices (no extra files, persistent across reboots)
	mmcDevices := []string{"mmcblk0", "mmcblk1"}
	for _, dev := range mmcDevices {
		cidPath := fmt.Sprintf("/sys/block/%s/device/cid", dev)
		if data, err := os.ReadFile(cidPath); err == nil {
			cid := strings.TrimSpace(string(data))
			if cid == "" {
				continue
			}
			// Use last 12 hex chars for a shorter, readable suffix
			if len(cid) > 12 {
				cid = cid[len(cid)-12:]
			}
			id := fmt.Sprintf("milkv-%s", strings.ToLower(cid))
			logger.Infof("Derived node ID from %s: %s", cidPath, id)
			return id
		}
	}

	// 3) Derive from MAC address
	// NOTE: We intentionally do NOT require the interface to be UP.
	// On Buildroot / early boot, the NIC may be DOWN when this runs,
	// but the MAC address in sysfs is still stable and persistent.
	if ifaces, err := net.Interfaces(); err == nil {
		for _, iface := range ifaces {
			// Skip loopback interfaces only
			if iface.Flags&net.FlagLoopback != 0 {
				continue
			}
			mac := iface.HardwareAddr.String()
			if mac == "" {
				continue
			}
			// Clean MAC (remove colons) and use as stable suffix
			cleanMAC := strings.ReplaceAll(mac, ":", "")
			id := fmt.Sprintf("milkv-%s", strings.ToLower(cleanMAC))
			logger.Infof("Derived node ID from MAC %s on interface %s: %s", mac, iface.Name, id)
			return id
		}
	}

	// 4) Fallback to random suffix
	fallback := fmt.Sprintf("milkv-%d", time.Now().UnixNano()%100000000)
	logger.Warnf("Failed to derive stable node ID from any hardware ID; using fallback ID: %s", fallback)
	return fallback
}

func main() {
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)

	nodeID := getStableNodeID(logger)

	memoryLimitStr := strings.TrimSpace(os.Getenv("MEMORY_LIMIT"))
	var memoryLimit int
	if memoryLimitStr != "" {
		// Support "256MB", "256 MB", "256mb", "256" (parse leading digits only)
		_, _ = fmt.Sscanf(memoryLimitStr, "%d", &memoryLimit)
		if memoryLimit <= 0 {
			trimmed := strings.TrimSuffix(strings.TrimSuffix(strings.ToUpper(memoryLimitStr), "MB"), " M")
			memoryLimit, _ = strconv.Atoi(strings.TrimSpace(trimmed))
		}
		if memoryLimit <= 0 {
			memoryLimit = 128
		}
	} else {
		if totalMB, _ := getSystemMemoryMB(logger, 128); totalMB > 0 {
			memoryLimit = totalMB
		} else {
			memoryLimit = 128
		}
	}

	edgeServer := os.Getenv("EDGE_SERVER")
	apiBase := os.Getenv("EDGE_API_BASE")
	if edgeServer == "" || apiBase == "" {
		if serfAddr, apiURL, ok := discoverEdgeViaMDNS(logger); ok {
			if edgeServer == "" {
				edgeServer = serfAddr
			}
			if apiBase == "" {
				apiBase = apiURL
			}
		}
	}
	if edgeServer == "" {
		edgeServer = "edge-server:8000"
	}
	if apiBase == "" {
		apiBase = "http://edge-server:8081"
	}

	logger.Infof("Starting MilkV node: %s with %dMB memory", nodeID, memoryLimit)

	taskWorkDir := os.Getenv("TASK_WORKDIR")
	if taskWorkDir == "" {
		const defaultWorkDir = "/root/sg2000"
		if info, err := os.Stat(defaultWorkDir); err == nil && info.IsDir() {
			taskWorkDir = defaultWorkDir
			logger.Infof("TASK_WORKDIR not set; defaulting to %s", defaultWorkDir)
		}
	}
	if taskWorkDir == "" {
		if wd, err := os.Getwd(); err == nil {
			taskWorkDir = wd
		} else {
			taskWorkDir = "."
		}
	}
	logger.Infof("Task working directory: %s", taskWorkDir)

	node := &MilkvNode{
		nodeID:      nodeID,
		memoryLimit: memoryLimit,
		logger:      logger,
		serfJoin:    edgeServer,
		edgeAPIBase: strings.TrimRight(apiBase, "/"),
		taskWorkDir: taskWorkDir,
		nodeIP:      getStableNodeIP(logger),
	}

	if err := node.advertiseMDNS(); err != nil {
		logger.Warnf("mDNS advertise failed (continuing anyway): %v", err)
	}

	if err := node.initSerf(node.serfJoin); err != nil {
		logger.Fatalf("Failed to initialize Serf: %v", err)
	}

	node.sendNodeInfo()
	go node.startMetricsLoop()
	go node.startTaskProcessor()

	if os.Getenv("MQTT_BROKER") == "" {
		if u, err := url.Parse(node.edgeAPIBase); err == nil {
			host := u.Hostname()
			if host != "" {
				def := fmt.Sprintf("tcp://%s:%s", host, "1883")
				os.Setenv("MQTT_BROKER", def)
				logger.Infof("MQTT_BROKER not set; defaulting to %s", def)
			}
		}
	}
	if mqttClient, err := node.initMQTT(); err != nil {
		logger.Warnf("MQTT init failed (continuing with HTTP polling): %v", err)
	} else {
		node.nodeMQTT = mqttClient
		node.useMQTT = true
		logger.Info("MQTT active: HTTP polling will be disabled")
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	logger.Infof("Received signal %v, shutting down", sig)
	node.shutdown()
}

// initSerf initializes Serf and joins the cluster
func (m *MilkvNode) initSerf(edgeServer string) error {
	config := serf.DefaultConfig()
	config.NodeName = m.nodeID
	config.MemberlistConfig.BindAddr = "0.0.0.0"
	config.MemberlistConfig.BindPort = 0
	config.MemberlistConfig.ProbeInterval = 5 * time.Second
	config.MemberlistConfig.ProbeTimeout = 3 * time.Second
	config.MemberlistConfig.PushPullInterval = 30 * time.Second
	config.MemberlistConfig.TCPTimeout = 10 * time.Second
	config.MemberlistConfig.SuspicionMult = 4

	// Listen for Serf user events (task cancel).
	m.eventCh = make(chan serf.Event, 64)
	config.EventCh = m.eventCh

	serf, err := serf.Create(config)
	if err != nil {
		return err
	}

	m.serf = serf

	if _, err = serf.Join([]string{edgeServer}, true); err != nil {
		return fmt.Errorf("failed to join edge Serf at %s: %w", edgeServer, err)
	}
	m.logger.Infof("Successfully joined cluster at %s", edgeServer)

	go m.handleSerfEvents()
	go m.startJobScheduler()

	return nil
}

func (m *MilkvNode) handleSerfEvents() {
	for evt := range m.eventCh {
		switch e := evt.(type) {
		case serf.UserEvent:
			if e.Name != "task-cancel" {
				continue
			}
			var payload struct {
				NodeID string `json:"node_id"`
				TaskID string `json:"task_id"`
			}
			if err := json.Unmarshal(e.Payload, &payload); err != nil {
				continue
			}
			if payload.NodeID != "" && payload.NodeID != m.nodeID {
				continue
			}
			if payload.TaskID == "" {
				continue
			}
			m.requestCancel(payload.TaskID)
		}
	}
}

func (m *MilkvNode) requestCancel(taskID string) {
	m.cancelMu.Lock()
	defer m.cancelMu.Unlock()
	if m.currentCancel != nil && m.currentCancelTask == taskID {
		m.logger.Warnf("Cancellation requested for task %s", taskID)
		m.currentCancel()
	}
}

// getStableNodeIP returns a stable IP for the node:
// 1) NODE_IP env var if set
// 2) Primary outbound IP (UDP dial trick)
// 3) First non-loopback, up interface IPv4
func getStableNodeIP(logger *logrus.Logger) string {
	if envIP := strings.TrimSpace(os.Getenv("NODE_IP")); envIP != "" {
		logger.Infof("Using NODE_IP override: %s", envIP)
		return envIP
	}

	if ifaces, err := net.Interfaces(); err == nil {
		var fallbackIP net.IP
		for _, iface := range ifaces {
			if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
				continue
			}
			addrs, err := iface.Addrs()
			if err != nil {
				continue
			}
			for _, addr := range addrs {
				if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
					if ip4 := ipnet.IP.To4(); ip4 != nil {
						// Skip link-local (169.254/16)
						if ip4[0] == 169 && ip4[1] == 254 {
							continue
						}
						// Prefer 192.168/16
						if ip4[0] == 192 && ip4[1] == 168 {
							return ip4.String()
						}
						// Remember first usable non-loopback IPv4 as fallback
						if fallbackIP == nil {
							fallbackIP = ip4
						}
					}
				}
			}
		}
		if fallbackIP != nil {
			return fallbackIP.String()
		}
	}

	logger.Warn("Unable to determine stable node IP; defaulting to empty")
	return ""
}

// startMetricsLoop periodically refreshes Serf/MQTT metrics.
// Keeping this as a single low-frequency loop avoids per-task high-frequency tag updates (which can grow memory).
func (m *MilkvNode) startMetricsLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		m.updateMemoryTags()
	}
}

// sendNodeInfo sends node information to the cluster via Serf tags
func (m *MilkvNode) sendNodeInfo() {
	var totalMB, availMB int
	if m.memoryLimit > 0 {
		totalMB = m.memoryLimit
		availMB = totalMB
		if task := m.getCurrentTask(); task != nil {
			availMB = totalMB - task.MemoryUsage
			if availMB < 0 {
				availMB = 0
			}
		}
	} else {
		totalMB, availMB = getSystemMemoryMB(m.logger, 0)
	}
	if strings.TrimSpace(os.Getenv("SIMULATE")) == "1" && m.memoryLimit > 0 {
		totalMB = m.memoryLimit
		availMB = m.memoryLimit
		if task := m.getCurrentTask(); task != nil {
			availMB = totalMB - task.MemoryUsage
			if availMB < 0 {
				availMB = 0
			}
		}
	}

	// Cache IP if not already set
	if m.nodeIP == "" {
		m.nodeIP = getStableNodeIP(m.logger)
	}

	tags := map[string]string{
		"memory_limit": fmt.Sprintf("%d", totalMB),
		"free_memory":  fmt.Sprintf("%d", availMB),
		"node_type":    "milkv",
		"status":       "online",
	}

	// Add IP address to tags if available
	if m.nodeIP != "" {
		tags["ip_address"] = m.nodeIP
		m.logger.Infof("Node IP address: %s", m.nodeIP)
	}

	m.serf.SetTags(tags)
	m.logger.Infof("Updated Serf tags: memory_limit=%d, free_memory=%d, ip_address=%s", totalMB, availMB, m.nodeIP)
}

// startTaskProcessor periodically checks and processes tasks
func (m *MilkvNode) startTaskProcessor() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if m.getCurrentTask() != nil {
			m.processTask()
		}
	}
}

// processTask executes the current task
func (m *MilkvNode) processTask() {
	task := m.getCurrentTask()
	if task == nil {
		return
	}

	m.logger.Infof("Processing task: %s (%s)", task.ID, task.Name)

	// Ensure binaries and models exist before processing task
	if strings.TrimSpace(os.Getenv("SIMULATE")) != "1" {
		if err := m.ensureBinariesAndModels(); err != nil {
			m.logger.Warnf("Failed to ensure binaries/models: %v (continuing anyway)", err)
		}
	}

	// Handle file-based tasks: download and convert if needed
	if task.InputMode == shared.InputModeFile {
		if err := m.prepareFileForTask(task); err != nil {
			m.logger.Errorf("Failed to prepare file for task %s: %v", task.ID, err)
			// Ensure StartedAt is set to avoid nil deref in completion
			if task.StartedAt == nil {
				now := time.Now()
				task.StartedAt = &now
			}
			m.finalizeTaskFailure(task, time.Now(), err)
			return
		}
	}

	if m.nodeMQTT != nil {
		m.nodeMQTT.publishStatus(task.ID, "started")
	}

	startTime := time.Now()
	duration := time.Duration(task.Duration) * time.Second // Thoi gian mo phong
	if task.InputMode == shared.InputModeFile {
		// File/H264 mode should run until the binary exits (no duration-based timeout).
		duration = 0
	} else if duration <= 0 {
		duration = 30 * time.Second
	}
	startedCopy := startTime
	task.StartedAt = &startedCopy

	var execErr error
	// Set up cancellation for the running workload.
	var baseCtx context.Context
	baseCtx = context.Background()
	ctx, cancel := context.WithCancel(baseCtx)
	m.cancelMu.Lock()
	m.currentCancel = cancel
	m.currentCancelTask = task.ID
	m.cancelMu.Unlock()
	defer func() {
		m.cancelMu.Lock()
		if m.currentCancelTask == task.ID {
			m.currentCancel = nil
			m.currentCancelTask = ""
		}
		m.cancelMu.Unlock()
	}()

	if strings.TrimSpace(os.Getenv("SIMULATE")) == "1" {
		speed := 1.0
		if v := strings.TrimSpace(os.Getenv("NODE_SPEED")); v != "" {
			if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
				speed = f
			}
		}
		// Completion time inversely proportional to node power: stronger node => shorter duration
		simSec := simulatedDurationSecForTaskType(task.Type) * float64(task.Duration)
		reduction := 0.0
		if m.lastType == task.Type && task.Type != 0 {
			reduction = 0.4
		}
		simSec -= reduction
		if simSec < 0 {
			simSec = 0
		}
		if speed < 0.1 {
			speed = 0.1
		}
		simDur := time.Duration(simSec/speed) * time.Second
		execErr = m.runSimulatedWorkload(ctx, task, simDur)
	} else if spec, ok := m.commandSpecForTask(task); ok {
		execErr = m.runExternalWorkload(ctx, task, duration, spec)
	} else {
		execErr = m.runSimulatedWorkload(ctx, task, duration)
	}

	if execErr != nil {
		if execErr == errTaskCancelled {
			m.finalizeTaskStopped(task, startTime)
			return
		}
		m.finalizeTaskFailure(task, startTime, execErr)
		return
	}

	m.finalizeTaskSuccess(task, startTime)
}

type taskCommandSpec struct {
	command string
	args    []string
}

func (m *MilkvNode) commandSpecForTask(task *shared.Task) (*taskCommandSpec, bool) {
	var args []string

	switch task.Type {
	case shared.TaskHeatmap:
		if task.InputMode == shared.InputModeFile {
			args = []string{"--model", "yolov8n_cv181x_int8_sym.cvimodel", "--h264", task.FilePath}
			return &taskCommandSpec{command: "./sample_vi_heatmap_h264", args: args}, true
		}
		args = []string{"yolov8n_cv181x_int8_sym.cvimodel"}
		return &taskCommandSpec{command: "./sample_vi_heatmap_new", args: args}, true

	case shared.TaskDrowsiness:
		if task.InputMode == shared.InputModeFile {
			args = []string{"--model", "drowsiness_cv181x_int8_sym.cvimodel", "--h264", task.FilePath, "--out", "./metadata.txt"}
			return &taskCommandSpec{command: "./sample_vi_drowsiness_h264", args: args}, true
		}
		args = []string{"drowsiness_cv181x_int8_sym.cvimodel"}
		return &taskCommandSpec{command: "./sample_vi_drowsiness", args: args}, true

	case shared.TaskPeopleCounting:
		if task.InputMode == shared.InputModeFile {
			args = []string{task.FilePath, "1920", "1080", "yolov8n_cv181x_int8_sym.cvimodel", "NULL"}
			return &taskCommandSpec{command: "./sample_vi_counting_h264", args: args}, true
		}
		args = []string{"yolov8n_cv181x_int8_sym.cvimodel", "NULL"}
		return &taskCommandSpec{command: "./sample_vi_counting", args: args}, true

	// Các loại mới (5-10) chưa có binary thực tế → dùng simulated (có thể thêm sau)
	default: // 5-10
		m.logger.Infof("Task type %d chưa có binary thực tế → dùng simulated workload", task.Type)
		return nil, false
	}
}

func (m *MilkvNode) runExternalWorkload(parentCtx context.Context, task *shared.Task, timeout time.Duration, spec *taskCommandSpec) error {
	// For file/H264 mode: no timeout (run until finished).
	// For camera mode: duration controls when we stop the binary (SIGINT then kill).
	if task.InputMode != shared.InputModeFile && timeout <= 0 {
		timeout = 10 * time.Minute
	}
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	if task.InputMode == shared.InputModeFile {
		ctx = context.Background()
		cancel = func() {}
	} else {
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
	}
	defer cancel()
	// For file-based tasks, update the file path in args with the actual path from prepareFileForTask
	if task.InputMode == shared.InputModeFile && task.FilePath != "" {
		// Replace file path in args with actual absolute path
		for i, arg := range spec.args {
			if arg == "--h264" && i+1 < len(spec.args) {
				// Next argument is the file path - replace with actual path
				spec.args[i+1] = task.FilePath
				break
			}
		}
	}

	cmd := exec.Command(spec.command, spec.args...)
	if m.taskWorkDir != "" {
		cmd.Dir = m.taskWorkDir
	}
	cmd.Env = os.Environ()

	var err error

	logDir := filepath.Join(m.taskWorkDir, "logs")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}

	// Rotate logs to keep only 5 most recent
	m.rotateLogs(logDir)

	logPath := filepath.Join(logDir, fmt.Sprintf("%s.log", task.ID))
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}
	defer logFile.Close()

	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if task.InputMode == shared.InputModeFile {
		m.logger.Infof("Executing external command for task %s (no timeout): %s %s", task.ID, spec.command, strings.Join(spec.args, " "))
	} else {
		m.logger.Infof("Executing external command for task %s: %s %s (timeout: %s)", task.ID, spec.command, strings.Join(spec.args, " "), timeout)
	}
	m.updateMemoryTags()

	if err = cmd.Start(); err != nil {
		return fmt.Errorf("failed to start command: %w", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	const sigintGrace = 5 * time.Second
	timedOut := false

	select {
	case err = <-done:
	case <-parentCtx.Done():
		timedOut = false
		m.logger.Warnf("Task %s cancelled; sending SIGINT", task.ID)
		if sigErr := cmd.Process.Signal(os.Interrupt); sigErr != nil {
			m.logger.Warnf("Failed to send SIGINT to task %s: %v", task.ID, sigErr)
		}
		select {
		case err = <-done:
		case <-time.After(sigintGrace):
			m.logger.Warnf("Task %s did not exit after cancel; killing", task.ID)
			if killErr := cmd.Process.Kill(); killErr != nil {
				m.logger.Warnf("Failed to kill task %s: %v", task.ID, killErr)
			}
			err = <-done
		}
		return errTaskCancelled
	case <-ctx.Done():
		if task.InputMode == shared.InputModeFile {
			// Should never happen (no timeout context), but be safe.
			err = <-done
			break
		}
		timedOut = true
		m.logger.Warnf("Task %s exceeded timeout %s; sending SIGINT", task.ID, timeout)
		if sigErr := cmd.Process.Signal(os.Interrupt); sigErr != nil {
			m.logger.Warnf("Failed to send SIGINT to task %s: %v", task.ID, sigErr)
		}

		select {
		case err = <-done:
			m.logger.Infof("Task %s exited after SIGINT", task.ID)
		case <-time.After(sigintGrace):
			m.logger.Warnf("Task %s did not exit after SIGINT; killing", task.ID)
			if killErr := cmd.Process.Kill(); killErr != nil {
				m.logger.Warnf("Failed to kill task %s: %v", task.ID, killErr)
			}
			err = <-done
		}
	}

	m.updateMemoryTags()

	m.logger.Infof("Task %s output written to %s", task.ID, logPath)

	if timedOut {
		// Reaching the timeout is treated as a normal, requested end of work.
		// This matches manual Ctrl+C behaviour: stop after duration, but count as success.
		m.logger.Infof("Task %s reached configured duration %s; treating timeout as normal completion", task.ID, timeout)
		return nil
	}
	if err != nil {
		return fmt.Errorf("command failed: %w", err)
	}
	return nil
}

var errTaskCancelled = fmt.Errorf("task cancelled")

// simulatedDurationSecForTaskType returns the simulated execution time in seconds per task type.
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

func (m *MilkvNode) runSimulatedWorkload(ctx context.Context, task *shared.Task, duration time.Duration) error {
	var buf []byte
	if task.MemoryUsage > 0 {
		allocBytes := task.MemoryUsage * 1024 * 1024
		if allocBytes > 0 {
			buf = make([]byte, allocBytes)
			pageSize := 4096
			for i := 0; i < len(buf); i += pageSize {
				buf[i] = byte(i % 256)
				if i+pageSize < len(buf) {
					buf[i+pageSize-1] = byte((i + pageSize - 1) % 256)
				}
			}
			runtime.GC()
			runtime.KeepAlive(buf)
			time.Sleep(100 * time.Millisecond)
		}
	}

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(duration)
	defer deadline.Stop()

	for {
		select {
		case <-ctx.Done():
			buf = nil
			debug.FreeOSMemory()
			m.updateMemoryTags()
			return errTaskCancelled
		case <-ticker.C:
			if len(buf) > 0 {
				buf[0]++
				buf[len(buf)/2]++
				buf[len(buf)-1]++
			}
		case <-deadline.C:
			buf = nil
			debug.FreeOSMemory()
			m.updateMemoryTags()
			return nil
		}
	}
}

func (m *MilkvNode) finalizeTaskSuccess(task *shared.Task, started time.Time) {
	now := time.Now()
	task.Status = shared.TaskStatusCompleted
	task.CompletedAt = &now

	m.logger.Infof("Task %s completed in %v", task.ID, time.Since(started))

	// Cleanup downloaded/temporary H264 artifacts after file-based inference
	m.cleanupDownloadedFileArtifacts(task)

	m.setCurrentTask(nil)
	m.updateMemoryTags()

	m.sendTaskCompletion(task, string(task.Status))
	if m.nodeMQTT != nil {
		m.nodeMQTT.publishCompletion(task, string(task.Status))
	}
}

func (m *MilkvNode) finalizeTaskFailure(task *shared.Task, started time.Time, execErr error) {
	now := time.Now()
	task.Status = shared.TaskStatusFailed
	task.CompletedAt = &now

	m.logger.Errorf("Task %s failed after %v: %v", task.ID, time.Since(started), execErr)

	// Cleanup downloaded/temporary H264 artifacts after file-based inference (even on failure)
	m.cleanupDownloadedFileArtifacts(task)

	m.setCurrentTask(nil)
	m.updateMemoryTags()

	m.sendTaskCompletion(task, string(task.Status))
	if m.nodeMQTT != nil {
		m.nodeMQTT.publishCompletion(task, string(task.Status))
	}
}

func (m *MilkvNode) finalizeTaskStopped(task *shared.Task, started time.Time) {
	now := time.Now()
	task.Status = shared.TaskStatusStopped
	task.CompletedAt = &now

	m.logger.Warnf("Task %s stopped after %v", task.ID, time.Since(started))

	// Cleanup downloaded/temporary H264 artifacts after file-based inference
	m.cleanupDownloadedFileArtifacts(task)

	m.setCurrentTask(nil)
	m.updateMemoryTags()

	m.sendTaskCompletion(task, string(task.Status))
	if m.nodeMQTT != nil {
		m.nodeMQTT.publishCompletion(task, string(task.Status))
	}
}

// cleanupDownloadedFileArtifacts deletes downloaded/temporary input files after file-based inference.
// We only delete files that we created ourselves:
// - They live under /root/h264/
// - Their basename is prefixed with "<task-id>_" (see prepareFileForTask download naming)
// This avoids deleting user-provided local files.
func (m *MilkvNode) cleanupDownloadedFileArtifacts(task *shared.Task) {
	if task == nil || task.InputMode != shared.InputModeFile {
		return
	}

	// Delete any file we created under /root/h264 for this task.
	// This covers:
	// - downloaded input: /root/h264/<taskID>_<name>.mp4/.h264
	// - converted output: /root/h264/<taskID>_<name>.h264
	pattern := filepath.Join("/root/h264", task.ID+"_*")
	matches, _ := filepath.Glob(pattern)
	if len(matches) == 0 {
		// Fallback: try to delete task.FilePath if it looks like a temp file.
		p := strings.TrimSpace(task.FilePath)
		if strings.HasPrefix(p, "/root/h264/") && strings.HasPrefix(filepath.Base(p), task.ID+"_") {
			matches = append(matches, p)
		}
	}

	for _, p := range matches {
		if err := os.Remove(p); err != nil {
			if !os.IsNotExist(err) {
				m.logger.Warnf("Failed to delete temporary file %s: %v", p, err)
			}
			continue
		}
		m.logger.Infof("Deleted temporary file: %s", p)
	}
}

// sendTaskCompletion sends a task completion event via Serf
func (m *MilkvNode) sendTaskCompletion(task *shared.Task, status string) {
	if m.serf == nil {
		return
	}

	durationSec := 0.0
	if task.StartedAt != nil {
		durationSec = time.Since(*task.StartedAt).Seconds()
	}

	completion := map[string]interface{}{
		"node_id":   m.nodeID,
		"task_id":   task.ID,
		"status":    status,
		"duration":  durationSec,
		"task_type": int(task.Type),
	}

	data, err := json.Marshal(completion)
	if err != nil {
		m.logger.Errorf("Failed to marshal task completion: %v", err)
		return
	}

	if err := m.serf.UserEvent("task-completion", data, false); err != nil {
		m.logger.Warnf("Failed to send task completion event: %v", err)
	}
}

// startJobScheduler periodically checks for available jobs
func (m *MilkvNode) startJobScheduler() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		m.checkForAvailableJobs()
	}
}

// checkForAvailableJobs fetches and claims available jobs from the edge server
func (m *MilkvNode) checkForAvailableJobs() {
	if m.getCurrentTask() != nil {
		return
	}
	if m.useMQTT {
		return
	}

	apiURL := m.edgeAPIBase + "/api/jobs/available"
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(apiURL)
	if err != nil {
		m.logger.Errorf("Failed to get available jobs: %v", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		m.logger.Errorf("Failed to get available jobs, status: %d", resp.StatusCode)
		return
	}
	var availableJobs []shared.Task
	if err := json.NewDecoder(resp.Body).Decode(&availableJobs); err != nil {
		m.logger.Errorf("Failed to decode available jobs: %v", err)
		return
	}

	availableMemory := m.memoryLimit
	if task := m.getCurrentTask(); task != nil {
		availableMemory -= task.MemoryUsage
	}

	for _, job := range availableJobs {
		if job.MemoryUsage <= availableMemory {
			if m.claimJob(job.ID) {
				break
			}
		}
	}
}

// claimJob attempts to claim a job from the edge server
func (m *MilkvNode) claimJob(jobID string) bool {
	apiURL := fmt.Sprintf("%s/api/jobs/%s/claim", m.edgeAPIBase, jobID)

	claimRequest := map[string]string{
		"node_id": m.nodeID,
	}

	jsonData, err := json.Marshal(claimRequest)
	if err != nil {
		m.logger.Errorf("Failed to marshal claim request: %v", err)
		return false
	}

	m.logger.Infof("Attempting to claim job %s from %s", jobID, apiURL)
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Post(apiURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		m.logger.Errorf("Failed to claim job %s: %v", jobID, err)
		return false
	}
	defer resp.Body.Close()

	m.logger.Infof("Job claim response status: %d", resp.StatusCode)

	if resp.StatusCode == http.StatusOK {
		var task shared.Task
		if err := json.NewDecoder(resp.Body).Decode(&task); err == nil {
			m.setCurrentTask(&task)
			m.logger.Infof("Successfully claimed and started job %s (%s, %dMB)", jobID, task.Name, task.MemoryUsage)
			m.updateMemoryTags()
			return true
		} else {
			m.logger.Errorf("Failed to decode task response: %v", err)
		}
	} else if resp.StatusCode == http.StatusConflict {
		m.logger.Infof("Job %s already claimed by another node", jobID)
		return false
	} else {
		m.logger.Warnf("Failed to claim job %s, status: %d", jobID, resp.StatusCode)
	}

	return false
}

// discoverEdgeViaMDNS discovers the edge server via mDNS
func discoverEdgeViaMDNS(logger *logrus.Logger) (string, string, bool) {
	const serviceType = "_edge-serf._tcp"
	resolver, err := zeroconf.NewResolver(nil)
	if err != nil {
		logger.Warnf("mDNS resolver init failed: %v", err)
		return "", "", false
	}

	entries := make(chan *zeroconf.ServiceEntry)
	foundSerf := ""
	foundAPI := ""

	go func() {
		for e := range entries {
			candidates := []string{}
			for _, a := range e.AddrIPv4 {
				candidates = append(candidates, a.String())
			}
			for _, a := range e.AddrIPv6 {
				ip := a.String()
				if parsed := net.ParseIP(ip); parsed != nil && parsed.To4() == nil {
					ip = "[" + ip + "]"
				}
				candidates = append(candidates, ip)
			}
			if len(candidates) == 0 {
				continue
			}

			var chosen string
			for _, h := range candidates {
				addr := net.JoinHostPort(h, strconv.Itoa(e.Port))
				conn, err := net.DialTimeout("tcp", addr, 600*time.Millisecond)
				if err == nil {
					conn.Close()
					chosen = h
					break
				}
			}
			if chosen == "" {
				chosen = candidates[0]
			}

			apiPort := "8081"
			for _, t := range e.Text {
				if strings.HasPrefix(t, "api=") {
					apiPort = strings.TrimPrefix(t, "api=")
				}
			}

			foundSerf = net.JoinHostPort(chosen, strconv.Itoa(e.Port))
			foundAPI = fmt.Sprintf("http://%s:%s", chosen, apiPort)
			logger.Infof("Discovered edge via mDNS: serf=%s api=%s", foundSerf, foundAPI)
			break
		}
	}()

	browseCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := resolver.Browse(browseCtx, serviceType, "local.", entries); err != nil {
		logger.Warnf("mDNS browse failed: %v", err)
		return "", "", false
	}
	<-browseCtx.Done()

	if foundSerf != "" && foundAPI != "" {
		return foundSerf, foundAPI, true
	}
	return "", "", false
}

// updateMemoryTags updates Serf tags with current memory and task information
func (m *MilkvNode) updateMemoryTags() {
	if m.serf == nil {
		return
	}

	task := m.getCurrentTask()
	var totalMB, availMB int
	if m.memoryLimit > 0 {
		// Always use configured limit so dashboard shows 256/128/384 etc., never host memory
		totalMB = m.memoryLimit
		availMB = totalMB
		if task != nil {
			availMB = totalMB - task.MemoryUsage
			if availMB < 0 {
				availMB = 0
			}
		}
	} else {
		totalMB, availMB = getSystemMemoryMB(m.logger, 0)
		if task != nil && totalMB > 0 && availMB == totalMB {
			availMB = totalMB - task.MemoryUsage
			if availMB < 0 {
				availMB = 0
			}
		}
	}

	// Ensure nodeIP is set and stable
	if m.nodeIP == "" {
		m.nodeIP = getStableNodeIP(m.logger)
	}

	status := "online"
	if task != nil {
		status = "busy"
	}

	tags := map[string]string{
		"memory_limit": fmt.Sprintf("%d", totalMB),
		"free_memory":  fmt.Sprintf("%d", availMB),
		"node_type":    "milkv",
		"status":       status,
	}

	// Advertise NODE_SPEED (used by scheduler when estimated_process_time is available).
	if v := strings.TrimSpace(os.Getenv("NODE_SPEED")); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			tags["node_speed"] = fmt.Sprintf("%.4f", f)
		}
	}

	if m.nodeIP != "" {
		tags["ip_address"] = m.nodeIP
	}

	if task != nil {
		tags["current_task_id"] = task.ID
		tags["current_task_name"] = task.Name
		tags["current_task_type"] = fmt.Sprintf("%d", task.Type)
		tags["current_task_memory"] = fmt.Sprintf("%d", task.MemoryUsage)
		tags["current_task_duration"] = fmt.Sprintf("%d", task.Duration)
		tags["current_task_input_mode"] = string(task.InputMode)
		if task.StartedAt != nil {
			tags["current_task_started"] = task.StartedAt.Format(time.RFC3339)
		}
	} else {
		tags["current_task_id"] = ""
		tags["current_task_name"] = ""
		tags["current_task_type"] = ""
		tags["current_task_memory"] = ""
		tags["current_task_duration"] = ""
		tags["current_task_input_mode"] = ""
		tags["current_task_started"] = ""
	}

	if err := m.serf.SetTags(tags); err != nil {
		m.logger.Warnf("Failed to update Serf tags: %v", err)
		return
	}

	if m.nodeMQTT != nil {
		// If you suspect MQTT is causing memory growth (QoS queues / slow broker),
		// you can disable metrics publishing without disabling MQTT job control:
		// DISABLE_MQTT_METRICS=1
		if strings.TrimSpace(os.Getenv("DISABLE_MQTT_METRICS")) == "1" {
			return
		}

		// Throttle metrics publishing to reduce allocations and avoid MQTT client queue growth.
		// Publish at most once every 5 seconds.
		const minInterval = int64(5 * int64(time.Second))
		nowNS := time.Now().UnixNano()
		lastNS := atomic.LoadInt64(&m.lastMQTTMetricsNS)
		if lastNS == 0 || (nowNS-lastNS) >= minInterval {
			atomic.StoreInt64(&m.lastMQTTMetricsNS, nowNS)

			taskID := ""
			taskName := ""
			inputMode := ""
			if task := m.getCurrentTask(); task != nil {
				taskID = task.ID
				taskName = task.Name
				inputMode = string(task.InputMode)
			}
			m.nodeMQTT.publishMetrics(totalMB, availMB, status, taskID, taskName, inputMode)
		}
	}
}

// getSystemMemoryMB returns (totalMB, availMB). When fallback (configured limit) is set,
// we always use it as total so the dashboard never shows host memory (e.g. 20GB).
// Used/avail come from cgroup when available, else total is used as free.
func getSystemMemoryMB(logger *logrus.Logger, fallback int) (int, int) {
	cgroupTotal, cgroupUsed := readCgroupMemoryMB(logger)

	if fallback > 0 {
		// Always report configured limit as total so each node shows 256/128/384 etc., not host.
		totalMB := fallback
		var availMB int
		// Only use cgroup usage when limit looks like a container (e.g. <= 8GB), not host (20GB+).
		if cgroupTotal > 0 && cgroupTotal <= 8192 && cgroupUsed >= 0 {
			availMB = totalMB - cgroupUsed
			if availMB < 0 {
				availMB = 0
			}
			if availMB > totalMB {
				availMB = totalMB
			}
		} else {
			availMB = totalMB
		}
		return totalMB, availMB
	}

	if cgroupTotal > 0 {
		availMB := cgroupTotal - cgroupUsed
		if availMB < 0 {
			availMB = 0
		}
		return cgroupTotal, availMB
	}
	return readProcMeminfoMB(logger, fallback)
}

// readCgroupMemoryMB reads container memory limit and usage from cgroup v2 or v1.
// Returns (limitMB, usedMB) or (0, 0) if not in a cgroup or read fails.
func readCgroupMemoryMB(logger *logrus.Logger) (int, int) {
	// Try cgroup v2 first (memory.current, memory.max)
	if limitMB, usedMB := readCgroupV2MemoryMB(); limitMB > 0 {
		return limitMB, usedMB
	}
	// Fall back to cgroup v1 (memory.limit_in_bytes, memory.usage_in_bytes)
	return readCgroupV1MemoryMB(logger)
}

func readCgroupV2MemoryMB() (int, int) {
	base := cgroupV2Base()
	if base == "" {
		return 0, 0
	}
	limitBuf, err := os.ReadFile(filepath.Join(base, "memory.max"))
	if err != nil {
		return 0, 0
	}
	limitStr := strings.TrimSpace(string(limitBuf))
	// "max" means no limit (host)
	if limitStr == "max" || limitStr == "" {
		return 0, 0
	}
	limitBytes, err := strconv.ParseUint(limitStr, 10, 64)
	if err != nil || limitBytes == 0 {
		return 0, 0
	}
	usedBuf, err := os.ReadFile(filepath.Join(base, "memory.current"))
	if err != nil {
		return 0, 0
	}
	usedBytes, err := strconv.ParseUint(strings.TrimSpace(string(usedBuf)), 10, 64)
	if err != nil {
		return 0, 0
	}
	limitMB := int(limitBytes / (1024 * 1024))
	usedMB := int(usedBytes / (1024 * 1024))
	return limitMB, usedMB
}

// cgroupV2Base returns the cgroup v2 path for this process (e.g. /sys/fs/cgroup or /sys/fs/cgroup/system.slice/docker-xxx.scope).
func cgroupV2Base() string {
	data, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return "/sys/fs/cgroup"
	}
	// v2: "0::/path" or "0::/system.slice/docker-xxx.scope"
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Split(line, ":")
		if len(fields) >= 3 && fields[1] == "" && fields[2] != "" {
			path := strings.TrimPrefix(fields[2], "/")
			if path == "" {
				return "/sys/fs/cgroup"
			}
			return filepath.Join("/sys/fs/cgroup", path)
		}
	}
	return "/sys/fs/cgroup"
}

func readCgroupV1MemoryMB(logger *logrus.Logger) (int, int) {
	// Find our cgroup path from /proc/self/cgroup
	data, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return 0, 0
	}
	var memoryPath string
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Split(line, ":")
		if len(fields) < 3 {
			continue
		}
		// format: id:controller:path
		if strings.Contains(fields[1], "memory") {
			memoryPath = strings.TrimPrefix(fields[2], "/")
			break
		}
	}
	if memoryPath == "" {
		return 0, 0
	}
	base := filepath.Join("/sys/fs/cgroup/memory", memoryPath)
	limitBuf, err := os.ReadFile(filepath.Join(base, "memory.limit_in_bytes"))
	if err != nil {
		return 0, 0
	}
	limitBytes, err := strconv.ParseUint(strings.TrimSpace(string(limitBuf)), 10, 64)
	if err != nil {
		return 0, 0
	}
	// Very large value usually means no limit (host)
	if limitBytes > 1<<62 {
		return 0, 0
	}
	usedBuf, err := os.ReadFile(filepath.Join(base, "memory.usage_in_bytes"))
	if err != nil {
		return 0, 0
	}
	usedBytes, err := strconv.ParseUint(strings.TrimSpace(string(usedBuf)), 10, 64)
	if err != nil {
		return 0, 0
	}
	limitMB := int(limitBytes / (1024 * 1024))
	usedMB := int(usedBytes / (1024 * 1024))
	return limitMB, usedMB
}

// readProcMeminfoMB reads memory information from /proc/meminfo
func readProcMeminfoMB(logger *logrus.Logger, fallback int) (int, int) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		logger.Warnf("Failed to read /proc/meminfo: %v (using fallback %dMB)", err, fallback)
		return fallback, fallback
	}
	lines := strings.Split(string(data), "\n")
	var totalKB, availKB int
	for _, line := range lines {
		if strings.HasPrefix(line, "MemTotal:") {
			fmt.Sscanf(line, "MemTotal: %d kB", &totalKB)
			continue
		}
		if strings.HasPrefix(line, "MemAvailable:") {
			fmt.Sscanf(line, "MemAvailable: %d kB", &availKB)
			continue
		}
	}
	if totalKB == 0 {
		return fallback, fallback
	}
	if availKB == 0 {
		for _, line := range lines {
			if strings.HasPrefix(line, "MemFree:") {
				fmt.Sscanf(line, "MemFree: %d kB", &availKB)
				break
			}
		}
		if availKB == 0 {
			availKB = totalKB
		}
	}
	totalMB := totalKB / 1024
	availMB := availKB / 1024
	if totalMB < 0 {
		totalMB = fallback
	}
	if availMB < 0 {
		availMB = 0
	}
	return totalMB, availMB
}

// advertiseMDNS publishes this worker node via mDNS for edge server discovery
func (m *MilkvNode) advertiseMDNS() error {
	const serviceType = "_milkv-worker._tcp"
	port := 8001
	text := []string{
		fmt.Sprintf("node_id=%s", m.nodeID),
		fmt.Sprintf("memory_limit=%d", m.memoryLimit),
		"node_type=milkv",
	}

	server, err := zeroconf.Register(m.nodeID, serviceType, "local.", port, text, nil)
	if err != nil {
		return err
	}

	m.mdnsServer = server
	m.logger.Infof("mDNS advertised: worker %s (service: %s)", m.nodeID, serviceType)
	return nil
}

// prepareFileForTask downloads and converts file if needed
func (m *MilkvNode) prepareFileForTask(task *shared.Task) error {
	if task.FilePath == "" && task.FileURL == "" {
		return fmt.Errorf("no file path or URL specified for file-based task")
	}

	// Ensure required directories exist
	h264Dir := "/root/h264"
	if err := os.MkdirAll(h264Dir, 0755); err != nil {
		return fmt.Errorf("failed to create h264 directory: %w", err)
	}

	var localFilePath string
	var needsConversion bool

	// If file URL is provided, download it
	if task.FileURL != "" {
		downloadURL := task.FileURL

		// If the URL points to localhost/127.0.0.1, it refers to the MilkV itself (wrong).
		// Rewrite to the edge API base host, since the file is served by the edge server.
		if u, err := url.Parse(task.FileURL); err == nil {
			// Resolve relative URLs against the edge API base.
			if u.Host == "" {
				if base, berr := url.Parse(m.edgeAPIBase); berr == nil {
					u = base.ResolveReference(u)
				}
			}
			host := strings.ToLower(u.Hostname())
			if host == "localhost" || host == "127.0.0.1" {
				if base, berr := url.Parse(m.edgeAPIBase); berr == nil && base.Host != "" {
					u.Host = base.Host
					u.Scheme = base.Scheme
				}
			}
			downloadURL = u.String()
		}

		m.logger.Infof("Downloading file from %s", downloadURL)
		fileName := filepath.Base(task.FilePath)
		if fileName == "" {
			fileName = fmt.Sprintf("%s.h264", task.ID)
		}
		localFilePath = filepath.Join(h264Dir, fmt.Sprintf("%s_%s", task.ID, fileName))

		// Download file
		client := &http.Client{Timeout: 15 * time.Second}
		resp, err := client.Get(downloadURL)
		if err != nil {
			return fmt.Errorf("failed to download file: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			if resp.StatusCode == http.StatusNotFound {
				return fmt.Errorf("file not found on edge server: %s", downloadURL)
			}
			return fmt.Errorf("failed to download file: status %d", resp.StatusCode)
		}

		// Save file
		out, err := os.Create(localFilePath)
		if err != nil {
			return fmt.Errorf("failed to create file: %w", err)
		}
		defer out.Close()

		_, err = io.Copy(out, resp.Body)
		if err != nil {
			return fmt.Errorf("failed to save file: %w", err)
		}

		m.logger.Infof("Downloaded file to %s", localFilePath)
	} else {
		// Use local file path - assume it's already on the worker node
		if !filepath.IsAbs(task.FilePath) {
			localFilePath = filepath.Join(h264Dir, task.FilePath)
		} else {
			localFilePath = task.FilePath
		}
	}

	// Check if file exists
	if _, err := os.Stat(localFilePath); err != nil {
		return fmt.Errorf("file not found: %s", localFilePath)
	}

	// Check if conversion is needed (mp4 -> h264)
	if task.FileType == "mp4" {
		needsConversion = true
	} else {
		// Try to detect from extension
		ext := strings.ToLower(filepath.Ext(localFilePath))
		if ext == ".mp4" {
			needsConversion = true
		}
	}

	// Convert mp4 to h264 if needed
	if needsConversion {
		m.logger.Infof("Converting MP4 to H264: %s", localFilePath)
		h264Path := strings.TrimSuffix(localFilePath, filepath.Ext(localFilePath)) + ".h264"

		// Use ffmpeg to convert (if available)
		cmd := exec.Command("ffmpeg", "-i", localFilePath, "-c:v", "libx264", "-an", "-f", "h264", h264Path)
		if err := cmd.Run(); err != nil {
			m.logger.Warnf("ffmpeg conversion failed (may not be installed): %v", err)
			m.logger.Warnf("Attempting to use file as-is (may fail if not H264)")
			// Continue with original file - worker may handle it
		} else {
			m.logger.Infof("Converted to H264: %s", h264Path)
			localFilePath = h264Path
			// Remove original mp4 file
			os.Remove(strings.TrimSuffix(localFilePath, ".h264") + ".mp4")
		}
	}

	// Update task with final file path (relative to h264 directory for command)
	task.FilePath = localFilePath
	task.FileType = "h264" // Always h264 after conversion or if already h264

	return nil
}

// ensureBinariesAndModels downloads binaries and models from edge server if they don't exist
func (m *MilkvNode) ensureBinariesAndModels() error {
	// Required binaries and models
	binaries := []string{
		"sample_vi_heatmap_h264",
		"sample_vi_drowsiness_h264",
		"sample_vi_heatmap_new",
		"sample_vi_drowsiness",
		"sample_vi_counting_h264",
		"sample_vi_counting",
	}
	models := []string{
		"yolov8n_cv181x_int8_sym.cvimodel",
		"drowsiness_cv181x_int8_sym.cvimodel",
	}

	// Check and download binaries
	for _, bin := range binaries {
		binPath := filepath.Join(m.taskWorkDir, bin)
		if _, err := os.Stat(binPath); os.IsNotExist(err) {
			m.logger.Infof("Binary %s not found, downloading from edge server", bin)
			if err := m.downloadResource(bin, binPath, true); err != nil {
				return fmt.Errorf("failed to download binary %s: %w", bin, err)
			}
			// Make executable
			os.Chmod(binPath, 0755)
		}
	}

	// Check and download models
	for _, model := range models {
		modelPath := filepath.Join(m.taskWorkDir, model)
		if _, err := os.Stat(modelPath); os.IsNotExist(err) {
			m.logger.Infof("Model %s not found, downloading from edge server", model)
			if err := m.downloadResource(model, modelPath, false); err != nil {
				return fmt.Errorf("failed to download model %s: %w", model, err)
			}
		}
	}

	return nil
}

// downloadResource downloads a resource file from the edge server
func (m *MilkvNode) downloadResource(resourceName, destPath string, isBinary bool) error {
	// Construct resource URL
	resourceURL := fmt.Sprintf("%s/api/resources/%s", m.edgeAPIBase, resourceName)

	m.logger.Infof("Downloading resource from %s to %s", resourceURL, destPath)

	resp, err := http.Get(resourceURL)
	if err != nil {
		return fmt.Errorf("failed to download resource: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to download resource: status %d", resp.StatusCode)
	}

	// Create destination directory if needed
	if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	// Save file
	out, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return fmt.Errorf("failed to save file: %w", err)
	}

	m.logger.Infof("Downloaded resource to %s", destPath)
	return nil
}

// rotateLogs keeps only the 5 most recent log files
func (m *MilkvNode) rotateLogs(logDir string) {
	entries, err := os.ReadDir(logDir)
	if err != nil {
		m.logger.Warnf("Failed to read log directory: %v", err)
		return
	}

	// Filter only .log files and get their info
	type logFileInfo struct {
		path    string
		modTime time.Time
	}
	var logFiles []logFileInfo

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".log") {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		logFiles = append(logFiles, logFileInfo{
			path:    filepath.Join(logDir, entry.Name()),
			modTime: info.ModTime(),
		})
	}

	// If we have 5 or fewer logs, no need to delete
	if len(logFiles) <= 5 {
		return
	}

	// Sort by modification time (newest first)
	for i := 0; i < len(logFiles)-1; i++ {
		for j := i + 1; j < len(logFiles); j++ {
			if logFiles[i].modTime.Before(logFiles[j].modTime) {
				logFiles[i], logFiles[j] = logFiles[j], logFiles[i]
			}
		}
	}

	// Delete oldest logs (keep 5 most recent)
	for i := 5; i < len(logFiles); i++ {
		if err := os.Remove(logFiles[i].path); err != nil {
			m.logger.Warnf("Failed to delete old log file %s: %v", logFiles[i].path, err)
		} else {
			m.logger.Infof("Deleted old log file: %s", logFiles[i].path)
		}
	}
}

// shutdown gracefully shuts down the worker node
func (m *MilkvNode) shutdown() {
	if m.mdnsServer != nil {
		m.mdnsServer.Shutdown()
	}
	if m.serf != nil {
		m.serf.Leave()
		m.serf.Shutdown()
	}
	m.logger.Info("MilkV node shutdown complete")
}
