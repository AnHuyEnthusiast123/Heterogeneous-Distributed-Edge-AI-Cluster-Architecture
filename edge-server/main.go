package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"cluster-docker/shared"

	"github.com/gorilla/websocket"
	"github.com/grandcat/zeroconf"
	"github.com/hashicorp/serf/serf"
	"github.com/sirupsen/logrus"
)

type EdgeServer struct {
	serf         *serf.Serf
	eventCh      chan serf.Event
	cluster      *ClusterManager
	webServer    *WebServer
	wsUpgrader   websocket.Upgrader
	clients      map[*websocket.Conn]bool
	clientsMux   sync.RWMutex
	broadcastMux sync.Mutex
	logger       *logrus.Logger
	clusterMQTT  *EdgeMQTT
}

type ClusterManager struct {
	nodes      map[string]*shared.Node
	jobQueue   []shared.Task
	mutex      sync.RWMutex
	logger     *logrus.Logger
	edgeServer *EdgeServer
	scheduler  *TaskScheduler

	// Evaluation / reporting
	jobHistory map[string]*JobRecord

	// Metrics (Prometheus + JSON export)
	metrics *EdgeMetrics
}

type WebServer struct {
	cluster *ClusterManager
	logger  *logrus.Logger
}

func main() {
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)

	cluster := &ClusterManager{
		nodes:      make(map[string]*shared.Node),
		jobQueue:   make([]shared.Task, 0),
		logger:     logger,
		scheduler:  NewTaskScheduler(logger),
		jobHistory: make(map[string]*JobRecord),
	}

	edgeServer := &EdgeServer{
		cluster: cluster,
		wsUpgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
		clients: make(map[*websocket.Conn]bool),
		logger:  logger,
	}

	cluster.edgeServer = edgeServer

	if err := edgeServer.initSerf(); err != nil {
		logger.Fatalf("Failed to initialize Serf: %v", err)
	}

	if err := edgeServer.advertiseMDNS(); err != nil {
		logger.Warnf("mDNS advertise failed (continuing with DNS): %v", err)
	}

	if mqttClient, err := edgeServer.initMQTT(); err != nil {
		logger.Warnf("MQTT init failed (continuing with HTTP): %v", err)
	} else {
		edgeServer.attachMQTT(mqttClient)
	}

	go edgeServer.discoverWorkersViaMDNS()

	edgeServer.webServer = &WebServer{
		cluster: cluster,
		logger:  logger,
	}
	edgeServer.webServer.setupRoutes()

	// Initialize and start API server
	apiServer := &APIServer{
		cluster: cluster,
		logger:  logger,
	}
	apiServer.setupAPIRoutes()
	go edgeServer.startAPIServer(apiServer)

	go edgeServer.startWebServer()

	// Metrics (compatible names with original PSO-ACO-GA_perTask.go)
	cluster.metrics = NewEdgeMetrics(logger, cluster)
	cluster.metrics.Start()

	// Periodically allow the scheduler to tune its PSO/ACO parameters (GA layer)
	// based on observed performance statistics.
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for range t.C {
			cluster.mutex.Lock()
			cluster.scheduler.UpdateParams()
			cluster.mutex.Unlock()
		}
	}()

	go func() {
		// Broadcast cluster state when Serf tags change.
		// Avoid ultra-high frequency broadcasts which can make the dashboard re-render so often that
		// buttons become hard to click and the page feels "stuck".
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()
		lastTags := make(map[string]map[string]string)
		for range t.C {
			members := edgeServer.serf.Members()
			changed := false
			for _, member := range members {
				if member.Status != serf.StatusAlive || member.Name == "edge-server" {
					continue
				}
				lastMemberTags, exists := lastTags[member.Name]
				if !exists {
					lastTags[member.Name] = make(map[string]string)
					for k, v := range member.Tags {
						lastTags[member.Name][k] = v
					}
					changed = true
					continue
				}
				for k, v := range member.Tags {
					if lastMemberTags[k] != v {
						changed = true
						lastTags[member.Name][k] = v
					}
				}
			}
			if changed {
				edgeServer.broadcastClusterState()
			}
		}
	}()

	logger.Info("Edge server started successfully")
	logger.Info("Web GUI: http://localhost:8080")

	select {}
}

// initSerf initializes the Serf cluster membership system
func (e *EdgeServer) initSerf() error {
	config := serf.DefaultConfig()
	config.NodeName = "edge-server"
	config.MemberlistConfig.BindAddr = "0.0.0.0"
	config.MemberlistConfig.BindPort = 8000
	config.LogOutput = io.Discard
	config.MemberlistConfig.LogOutput = io.Discard
	config.MemberlistConfig.ProbeInterval = 0
	config.MemberlistConfig.ProbeTimeout = 0
	config.MemberlistConfig.SuspicionMult = 0

	e.eventCh = make(chan serf.Event, 64)
	config.EventCh = e.eventCh

	serf, err := serf.Create(config)
	if err != nil {
		return err
	}

	e.serf = serf

	tags := map[string]string{
		"node_type":    "edge",
		"status":       "online",
		"memory_limit": "128",
		"free_memory":  "128",
	}
	e.serf.SetTags(tags)

	go e.handleSerfEvents()

	return nil
}

// handleSerfEvents processes Serf cluster events
func (e *EdgeServer) handleSerfEvents() {
	e.logger.Info("Starting Serf event handler")
	for event := range e.eventCh {
		switch evt := event.(type) {
		case serf.MemberEvent:
			if evt.Type == serf.EventMemberUpdate {
				e.broadcastClusterState()
			} else {
				e.logger.Infof("Processing MemberEvent: %v", evt.Type)
				e.handleMemberEvent(evt)
			}
		case serf.UserEvent:
			e.logger.Infof("Processing UserEvent: %s", evt.Name)
			e.handleUserEvent(evt)
		default:
			e.logger.Infof("Unknown event type: %T", event)
		}
	}
}

// handleMemberEvent processes Serf member join/leave events
func (e *EdgeServer) handleMemberEvent(event serf.MemberEvent) {
	for _, member := range event.Members {
		switch event.Type {
		case serf.EventMemberJoin:
			nodeName := member.Name // Capture loop variable
			e.logger.Infof("Node joined: %s", nodeName)
			e.cluster.addNode(nodeName, "milkv", 128)
			go func() {
				time.Sleep(2 * time.Second)
				if e.clusterMQTT != nil {
					e.clusterMQTT.republishAllPendingJobs(e.cluster)
				}
				e.broadcastClusterState()
			}()
		case serf.EventMemberLeave, serf.EventMemberFailed:
			e.logger.Infof("Node left: %s", member.Name)
			e.cluster.removeNode(member.Name)
		}
	}
	e.broadcastClusterState()
}

// handleUserEvent processes custom Serf user events
func (e *EdgeServer) handleUserEvent(event serf.UserEvent) {
	switch event.Name {
	case "node-info":
	case "task-completion":
		var completion struct {
			NodeID   string  `json:"node_id"`
			TaskID   string  `json:"task_id"`
			Status   string  `json:"status"`
			Duration float64 `json:"duration"`
			TaskType int     `json:"task_type,omitempty"`
		}
		if err := json.Unmarshal(event.Payload, &completion); err == nil {
			e.logger.Infof("Task %s completed on node %s in %.2f seconds (handled via Serf tags)",
				completion.TaskID, completion.NodeID, completion.Duration)

			// Metrics (Prometheus + JSON snapshot). Do not hold cluster lock here.
			if e.cluster != nil && e.cluster.metrics != nil && completion.NodeID != "" {
				e.cluster.metrics.OnTaskCompletion(completion.NodeID, shared.TaskStatus(completion.Status), completion.Duration)
			}

			// Update evaluation record.
			if e.cluster != nil && e.cluster.jobHistory != nil && completion.TaskID != "" {
				e.cluster.mutex.Lock()
				if rec := e.cluster.jobHistory[completion.TaskID]; rec != nil {
					rec.Status = shared.TaskStatus(completion.Status)
					rec.CompletedAt = time.Now()
					rec.DurationSec = completion.Duration
					if rec.AssignedTo == "" && completion.NodeID != "" {
						rec.AssignedTo = completion.NodeID
					}
					// Processing power is fixed per worker (set from fixed table if missing)
					if rec.NodeSpeed <= 0 && completion.NodeID != "" {
						rec.NodeSpeed = ProcessingPowerGHzForWorker(completion.NodeID)
					}
				}
				e.cluster.mutex.Unlock()
			}

			// Only learn from successful completions (ignore failed/stopped).
			if e.cluster != nil && e.cluster.scheduler != nil &&
				completion.Status == string(shared.TaskStatusCompleted) &&
				completion.NodeID != "" && completion.TaskType != 0 {
				e.cluster.mutex.Lock()
				e.cluster.scheduler.OnCompleted(completion.NodeID, shared.Task{}, completion.Duration)
				e.cluster.mutex.Unlock()
			}

			go func() {
				time.Sleep(200 * time.Millisecond)
				if e.clusterMQTT != nil {
					e.clusterMQTT.republishAllPendingJobs(e.cluster)
				}
				time.Sleep(100 * time.Millisecond)
				e.broadcastClusterState()
			}()
		}
	}
	e.broadcastClusterState()
}

// startWebServer starts the web UI server
func (e *EdgeServer) startWebServer() {
	e.logger.Info("Starting web server on :8080")
	mux := http.NewServeMux()
	mux.HandleFunc("/", e.webServer.dashboard)
	mux.HandleFunc("/ws", e.handleWebSocket)
	// Proxy go2rtc (often bound to 127.0.0.1:1984) through the edge server so the dashboard
	// can load it from any client machine using the same origin.
	mux.Handle("/go2rtc/", http.StripPrefix("/go2rtc", e.go2rtcReverseProxy()))
	mux.Handle("/api/", e.go2rtcReverseProxy())
	// go2rtc also serves metadata on /api (no trailing slash)
	mux.Handle("/api", e.go2rtcReverseProxy())
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("./static/"))))
	if err := http.ListenAndServe(":8080", mux); err != nil {
		e.logger.Fatalf("Web server failed: %v", err)
	}
}

func (e *EdgeServer) go2rtcReverseProxy() http.Handler {
	target, _ := url.Parse("http://127.0.0.1:1984")
	proxy := httputil.NewSingleHostReverseProxy(target)
	origDirector := proxy.Director
	proxy.Director = func(r *http.Request) {
		origDirector(r)
		r.Host = target.Host
		// go2rtc can enforce Origin checks for WebSocket and some API calls.
		// When proxied through edge-server, the browser Origin will be http://<edge>:8080,
		// so rewrite it to match the upstream.
		if r.Header.Get("Origin") != "" {
			r.Header.Set("Origin", target.Scheme+"://"+target.Host)
		}
		if r.Header.Get("Referer") != "" {
			r.Header.Set("Referer", target.Scheme+"://"+target.Host+"/")
		}
	}
	// Help streaming responses / websockets behave well through the proxy.
	proxy.FlushInterval = 100 * time.Millisecond
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		e.logger.Warnf("go2rtc proxy error for %s %s: %v", r.Method, r.URL.String(), err)
		http.Error(w, "go2rtc proxy error", http.StatusBadGateway)
	}
	// Allow embedding go2rtc UI/player in an iframe (the dashboard stream modal).
	// Some go2rtc builds set X-Frame-Options / CSP headers that prevent iframe embedding and
	// the UI will appear stuck on "loading".
	proxy.ModifyResponse = func(resp *http.Response) error {
		h := resp.Header
		h.Del("X-Frame-Options")
		h.Del("Content-Security-Policy")
		h.Del("Content-Security-Policy-Report-Only")
		h.Del("Cross-Origin-Opener-Policy")
		h.Del("Cross-Origin-Embedder-Policy")
		h.Del("Cross-Origin-Resource-Policy")
		return nil
	}
	return proxy
}

// startAPIServer starts the API server on port 8081
func (e *EdgeServer) startAPIServer(apiServer *APIServer) {
	e.logger.Info("Starting API server on :8081")
	if err := http.ListenAndServe(":8081", apiServer.router); err != nil {
		e.logger.Fatalf("API server failed: %v", err)
	}
}

// advertiseMDNS publishes the edge server via mDNS for worker discovery
func (e *EdgeServer) advertiseMDNS() error {
	const serviceType = "_edge-serf._tcp"
	port := 8000
	text := []string{
		"api=8081",
		"web=8080",
		"mqtt=1883",
	}

	server, err := zeroconf.Register("edge-server", serviceType, "local.", port, text, nil)
	if err != nil {
		return err
	}
	go func() {
		<-time.After(0)
		_ = server
	}()

	e.logger.Infof("mDNS advertised: service %s on port %d (api:8081 web:8080)", serviceType, port)
	return nil
}

// discoverWorkersViaMDNS continuously browses for worker nodes via mDNS
func (e *EdgeServer) discoverWorkersViaMDNS() {
	const serviceType = "_milkv-worker._tcp"
	resolver, err := zeroconf.NewResolver(nil)
	if err != nil {
		e.logger.Warnf("mDNS resolver init failed for worker discovery: %v", err)
		return
	}

	entries := make(chan *zeroconf.ServiceEntry)
	discoveredWorkers := make(map[string]bool)

	go func() {
		for entry := range entries {
			host := ""
			if len(entry.AddrIPv4) > 0 {
				host = entry.AddrIPv4[0].String()
			} else if len(entry.AddrIPv6) > 0 {
				host = entry.AddrIPv6[0].String()
			}
			if host == "" {
				continue
			}

			nodeID := ""
			memoryLimit := 128
			for _, txt := range entry.Text {
				if strings.HasPrefix(txt, "node_id=") {
					nodeID = strings.TrimPrefix(txt, "node_id=")
				} else if strings.HasPrefix(txt, "memory_limit=") {
					if mem, err := strconv.Atoi(strings.TrimPrefix(txt, "memory_limit=")); err == nil {
						memoryLimit = mem
					}
				}
			}

			if nodeID == "" {
				nodeID = entry.Instance
			}

			if discoveredWorkers[nodeID] {
				continue
			}

			discoveredWorkers[nodeID] = true
			e.logger.Infof("🔍 Auto-discovered worker via mDNS: %s at %s (memory: %dMB)", nodeID, host, memoryLimit)
			e.logger.Infof("   Worker will automatically join cluster via Serf when it discovers edge server")
		}
	}()

	ctx := context.Background()
	if err := resolver.Browse(ctx, serviceType, "local.", entries); err != nil {
		e.logger.Warnf("mDNS browse failed for worker discovery: %v", err)
		return
	}

	<-ctx.Done()
}

// handleWebSocket handles WebSocket connections for real-time UI updates
func (e *EdgeServer) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := e.wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		e.logger.Errorf("WebSocket upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	e.clientsMux.Lock()
	e.clients[conn] = true
	e.clientsMux.Unlock()

	state := e.cluster.getClusterState()
	data, err := json.Marshal(state)
	if err == nil {
		conn.WriteMessage(websocket.TextMessage, data)
	}

	for {
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			e.clientsMux.Lock()
			delete(e.clients, conn)
			e.clientsMux.Unlock()
			break
		}

		if messageType == websocket.TextMessage {
			var msg map[string]interface{}
			if err := json.Unmarshal(message, &msg); err == nil {
				if action, ok := msg["action"].(string); ok && action == "create_job" {
					var req shared.JobRequest
					if typeVal, ok := msg["type"].(float64); ok {
						req.Type = shared.TaskType(int(typeVal))
					}
					// Duration: JSON may decode as float64 or (rarely) as int
					if durationVal, ok := msg["duration"].(float64); ok {
						req.Duration = int(durationVal)
					} else if durationVal, ok := msg["duration"].(int); ok {
						req.Duration = durationVal
					}
					if inputModeVal, ok := msg["input_mode"].(string); ok {
						req.InputMode = shared.InputMode(inputModeVal)
					}
					if filePathVal, ok := msg["file_path"].(string); ok {
						req.FilePath = filePathVal
					}
					if fileTypeVal, ok := msg["file_type"].(string); ok {
						req.FileType = fileTypeVal
					}
					if fileURLVal, ok := msg["file_url"].(string); ok {
						req.FileURL = fileURLVal
					}

					if req.Type >= 1 && req.Type <= 4 && req.Duration > 0 {
						if e.clusterMQTT != nil {
							reqData, _ := json.Marshal(req)
							e.clusterMQTT.publishJobCreate(reqData)
						}
					}
				}
			}
		}
	}
}

// broadcastClusterState sends cluster state to all WebSocket clients
func (e *EdgeServer) broadcastClusterState() {
	e.broadcastMux.Lock()
	defer e.broadcastMux.Unlock()

	state := e.cluster.getClusterState()
	data, err := json.Marshal(state)
	if err != nil {
		e.logger.Errorf("Failed to marshal cluster state: %v", err)
		return
	}

	e.clientsMux.RLock()
	clientsCopy := make([]*websocket.Conn, 0, len(e.clients))
	for client := range e.clients {
		clientsCopy = append(clientsCopy, client)
	}
	e.clientsMux.RUnlock()

	for _, client := range clientsCopy {
		if err := client.WriteMessage(websocket.TextMessage, data); err != nil {
			if !isNormalCloseError(err) {
				e.logger.Warnf("Failed to send message to client: %v", err)
			}
			client.Close()
			e.clientsMux.Lock()
			delete(e.clients, client)
			e.clientsMux.Unlock()
		}
	}
}

// isNormalCloseError checks if an error is a normal WebSocket close
func isNormalCloseError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return errStr == "websocket: close 1001 (going away)" ||
		errStr == "websocket: close 1006 (abnormal closure)" ||
		errStr == "write: broken pipe" ||
		errStr == "write: connection reset by peer"
}

// addNode adds a new node to the cluster
func (cm *ClusterManager) addNode(nodeID, nodeType string, memoryLimit int) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	cm.nodes[nodeID] = &shared.Node{
		ID:          nodeID,
		Type:        nodeType,
		MemoryLimit: memoryLimit,
		FreeMemory:  memoryLimit,
		TaskQueue:   make([]shared.Task, 0),
		LastSeen:    time.Now(),
		Status:      shared.NodeStatusOnline,
	}

	if cm.scheduler != nil {
		cm.scheduler.EnsureNode(nodeID)
	}

	cm.logger.Infof("Added node: %s", nodeID)
}

// removeNode removes a node from the cluster and requeues its tasks
func (cm *ClusterManager) removeNode(nodeID string) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	if node, exists := cm.nodes[nodeID]; exists {
		if node.CurrentTask != nil {
			cm.jobQueue = append(cm.jobQueue, *node.CurrentTask)
		}
		cm.jobQueue = append(cm.jobQueue, node.TaskQueue...)
		delete(cm.nodes, nodeID)
		if cm.scheduler != nil {
			cm.scheduler.RemoveNode(nodeID)
		}
		cm.logger.Infof("Removed node: %s", nodeID)
	}
}

func (cm *ClusterManager) updateNodeMetrics(nodeID string, memoryLimit, freeMemory int, status string, currentTask *shared.Task) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	node, exists := cm.nodes[nodeID]
	if !exists {
		node = &shared.Node{
			ID:          nodeID,
			Type:        "milkv",
			TaskQueue:   make([]shared.Task, 0),
			MemoryLimit: memoryLimit,
		}
		cm.nodes[nodeID] = node
	}

	if memoryLimit > 0 {
		node.MemoryLimit = memoryLimit
	}
	node.FreeMemory = freeMemory
	node.LastSeen = time.Now()

	switch status {
	case "busy":
		node.Status = shared.NodeStatusBusy
	case "offline":
		node.Status = shared.NodeStatusOffline
	default:
		node.Status = shared.NodeStatusOnline
	}

	if currentTask != nil && currentTask.ID != "" {
		taskCopy := *currentTask
		node.CurrentTask = &taskCopy
	} else {
		node.CurrentTask = nil
	}
}

// getClusterState returns the current cluster state (thread-safe)
func (cm *ClusterManager) getClusterState() shared.ClusterState {
	// IMPORTANT: Never call Serf APIs while holding cm.mutex.
	// The Serf event handler goroutine can call into ClusterManager methods (locking cm.mutex),
	// and Serf itself may hold internal locks when invoking those callbacks.
	// If we call serf.Members() while holding cm.mutex we can create a lock inversion deadlock.

	// 1) Copy cluster-managed state under lock.
	cm.mutex.RLock()
	nodes := make(map[string]*shared.Node, len(cm.nodes))
	for id, node := range cm.nodes {
		nodeCopy := *node
		if len(node.TaskQueue) > 0 {
			queueCopy := make([]shared.Task, len(node.TaskQueue))
			copy(queueCopy, node.TaskQueue)
			nodeCopy.TaskQueue = queueCopy
		}
		if node.CurrentTask != nil {
			taskCopy := *node.CurrentTask
			nodeCopy.CurrentTask = &taskCopy
		}
		nodes[id] = &nodeCopy
	}
	jobQueueCopy := make([]shared.Task, len(cm.jobQueue))
	copy(jobQueueCopy, cm.jobQueue)
	edge := cm.edgeServer
	cm.mutex.RUnlock()

	// 2) Read Serf members without holding the cluster lock.
	var members []serf.Member
	if edge != nil && edge.serf != nil {
		members = edge.serf.Members()
	}

	// 3) Merge Serf member tags into the node map.
	for _, member := range members {
		if member.Status != serf.StatusAlive || member.Name == "edge-server" {
			continue
		}
		// IMPORTANT: Always merge Serf member tags into the node map.
		// We may already have a node record (e.g. from MQTT/job assignment),
		// but Serf tags are authoritative for current_task_* and ip_address used by the dashboard.
		node := nodes[member.Name]
		if node == nil {
			node = &shared.Node{
				ID:       member.Name,
				LastSeen: time.Now(),
			}
			nodes[member.Name] = node
		}

		// Extract IP address from member
		// Priority: 1) ip_address tag (explicit), 2) member.Addr (Serf connection IP), 3) member.Name
		nodeIP := ""
		if ipTag, hasIP := member.Tags["ip_address"]; hasIP && ipTag != "" {
			nodeIP = ipTag
		} else if len(member.Addr) > 0 {
			// Extract IP from member address (net.IP is a byte slice)
			// Prefer IPv4 if available
			if ip4 := member.Addr.To4(); ip4 != nil {
				nodeIP = ip4.String()
			} else {
				nodeIP = member.Addr.String()
			}
		} else {
			// Fallback: use member name (might be hostname or IP)
			nodeIP = member.Name
		}

		// Log IP source for debugging
		if ipTag, hasIP := member.Tags["ip_address"]; hasIP && ipTag != "" {
			// IP from tag - already set above
		} else if len(member.Addr) > 0 {
			cm.logger.Debugf("Node %s: Using Serf member.Addr IP: %s (tag was empty)", member.Name, nodeIP)
		} else {
			cm.logger.Debugf("Node %s: Using member.Name as IP fallback: %s", member.Name, nodeIP)
		}

		// Core fields (best-effort parse; keep previous values if tags missing)
		if nodeType, ok := member.Tags["node_type"]; ok && nodeType != "" {
			node.Type = nodeType
		}
		if memoryLimitStr, ok := member.Tags["memory_limit"]; ok {
			if v, err := strconv.Atoi(memoryLimitStr); err == nil {
				node.MemoryLimit = v
			}
		}
		if freeMemoryStr, ok := member.Tags["free_memory"]; ok {
			if v, err := strconv.Atoi(freeMemoryStr); err == nil {
				node.FreeMemory = v
			}
		}
		if status, ok := member.Tags["status"]; ok && status != "" {
			if status == "busy" {
				node.Status = shared.NodeStatusBusy
			} else {
				node.Status = shared.NodeStatusOnline
			}
		}
		node.IPAddress = nodeIP
		node.LastSeen = time.Now()

		if taskID, hasTaskID := member.Tags["current_task_id"]; hasTaskID && taskID != "" {
			if taskName, hasTaskName := member.Tags["current_task_name"]; hasTaskName {
				taskTypeStr := member.Tags["current_task_type"]
				taskMemoryStr := member.Tags["current_task_memory"]
				taskDurationStr := member.Tags["current_task_duration"]
				taskStartedStr := member.Tags["current_task_started"]
				taskInputModeStr := member.Tags["current_task_input_mode"]

				taskType := shared.TaskDrowsiness
				if taskTypeInt, err := strconv.Atoi(taskTypeStr); err == nil {
					taskType = shared.TaskType(taskTypeInt)
				}

				taskMemory := 0
				if taskMemoryInt, err := strconv.Atoi(taskMemoryStr); err == nil {
					taskMemory = taskMemoryInt
				}

				taskDuration := 0
				if taskDurationInt, err := strconv.Atoi(taskDurationStr); err == nil {
					taskDuration = taskDurationInt
				}

				var taskStarted *time.Time
				if taskStartedStr != "" {
					if parsedTime, err := time.Parse(time.RFC3339, taskStartedStr); err == nil {
						taskStarted = &parsedTime
					}
				}

				inputMode := shared.InputModeCamera
				if taskInputModeStr != "" {
					inputMode = shared.InputMode(taskInputModeStr)
				}

				node.CurrentTask = &shared.Task{
					ID:          taskID,
					Type:        taskType,
					Name:        taskName,
					MemoryUsage: taskMemory,
					Duration:    taskDuration,
					Status:      shared.TaskStatusRunning,
					StartedAt:   taskStarted,
					InputMode:   inputMode,
				}
			}
		} else {
			// Ensure the dashboard sees "Idle" when Serf clears current_task_id.
			node.CurrentTask = nil
		}

		nodes[member.Name] = node
	}

	// Recompute activeJobs from the final merged node map (prevents double-counting).
	activeJobs := 0
	for _, n := range nodes {
		if n == nil || n.ID == "edge-server" {
			continue
		}
		if n.CurrentTask != nil || n.Status == shared.NodeStatusBusy {
			activeJobs++
		}
	}

	// JobQueue in the UI represents "waiting for memory / unassigned" work.
	// Do not include running tasks here (those belong to node.CurrentTask).
	queuedJobs := make([]shared.Task, 0)
	for _, job := range jobQueueCopy {
		if job.Status == shared.TaskStatusPending {
			queuedJobs = append(queuedJobs, job)
		}
	}

	return shared.ClusterState{
		Nodes:      nodes,
		JobQueue:   queuedJobs,
		TotalJobs:  len(queuedJobs),
		ActiveJobs: activeJobs,
		LastUpdate: time.Now(),
	}
}

// addJob adds a new job to the queue
func (cm *ClusterManager) addJob(
	taskType shared.TaskType,
	duration int,
	inputMode shared.InputMode,
	filePath, fileType, fileURL string,
	configPath, configURL string,
	complexity, cpuReq, memReq, estProcSec, preTimeSec, powerFactor float64,
) string {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	taskName, memoryUsage := shared.GetTaskInfo(taskType)
	if memReq > 0 {
		memoryUsage = int(math.Round(memReq))
		if memoryUsage <= 0 {
			memoryUsage = 1
		}
	}
	task := shared.Task{
		ID:                   fmt.Sprintf("task-%d", time.Now().UnixNano()%100000000),
		Type:                 taskType,
		Name:                 taskName,
		MemoryUsage:          memoryUsage,
		Duration:             duration,
		Status:               shared.TaskStatusPending,
		CreatedAt:            time.Now(),
		ConfigPath:           configPath,
		ConfigURL:            configURL,
		Complexity:           complexity,
		CPUReq:               cpuReq,
		MemReq:               memReq,
		EstimatedProcessTime: estProcSec,
		PreTime:              preTimeSec,
		PowerFactor:          powerFactor,
		InputMode:            inputMode,
		FilePath:             filePath,
		FileType:             fileType,
		FileURL:              fileURL,
	}

	// Update display name if file-based
	if inputMode == shared.InputModeFile {
		task.Name = shared.GetTaskDisplayName(&task)
	}

	cm.jobQueue = append(cm.jobQueue, task)
	cm.logger.Infof("Added job: %s (mode: %s)", task.ID, inputMode)

	// Evaluation record
	if cm.jobHistory != nil {
		cm.jobHistory[task.ID] = &JobRecord{
			ID:           task.ID,
			Type:         task.Type,
			Name:         task.Name,
			MemoryUsage:  task.MemoryUsage,
			Duration:     task.Duration,
			InputMode:    task.InputMode,
			CreatedAt:    task.CreatedAt,
			Status:       task.Status,
			Complexity:   task.Complexity,
			DataSizeBits: task.DataSizeBits,
		}
	}

	return task.ID
}

// findBestWorkerForJob selects the best worker node for a job based on available memory
func (e *EdgeServer) findBestWorkerForJob(requiredMemory int) *serf.Member {
	members := e.serf.Members()
	var best *serf.Member
	maxFree := -1
	for _, m := range members {
		if m.Status != serf.StatusAlive {
			continue
		}
		if m.Name == "edge-server" {
			continue
		}
		nodeType, okType := m.Tags["node_type"]
		status, okStatus := m.Tags["status"]
		freeStr, okFree := m.Tags["free_memory"]
		if !okType || !okStatus || !okFree || nodeType != "milkv" {
			continue
		}
		if status == "busy" {
			continue
		}
		free, err := strconv.Atoi(freeStr)
		if err != nil || free < requiredMemory {
			continue
		}
		if free > maxFree || (free == maxFree && best != nil && m.Name < best.Name) || (best == nil) {
			copyM := m
			best = &copyM
			maxFree = free
		}
	}
	return best
}
