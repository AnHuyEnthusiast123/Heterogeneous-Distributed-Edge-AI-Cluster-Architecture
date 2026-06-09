//go:build mqtt
// +build mqtt

package main

import (
	"encoding/json"
	"os"
	"strconv"
	"time"

	"cluster-docker/shared"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/hashicorp/serf/serf"
	"github.com/sirupsen/logrus"
)

type EdgeMQTT struct {
	client      mqtt.Client
	topicJobs   string
	topicClaims string
	topicAssign string
	topicCreate string
	topicStatus string
	topicMetrics string
	logger      *logrus.Logger
}

// initMQTT initializes the MQTT client for the edge server
func (e *EdgeServer) initMQTT() (*EdgeMQTT, error) {
	broker := os.Getenv("MQTT_BROKER")
	if broker == "" {
		broker = "tcp://localhost:1883"
	}
	clientID := "edge-server"

	opts := mqtt.NewClientOptions().
		AddBroker(broker).
		SetClientID(clientID).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(2 * time.Second)

	if u := os.Getenv("MQTT_USERNAME"); u != "" {
		opts.SetUsername(u)
	}
	if p := os.Getenv("MQTT_PASSWORD"); p != "" {
		opts.SetPassword(p)
	}

	client := mqtt.NewClient(opts)
	if token := client.Connect(); !token.WaitTimeout(5*time.Second) || token.Error() != nil {
		return nil, token.Error()
	}

	m := &EdgeMQTT{
		client:       client,
		topicJobs:    "cluster/jobs",
		topicClaims:  "cluster/claims",
		topicAssign:  "cluster/assign/",
		topicCreate:  "cluster/jobs/create",
		topicStatus:  "cluster/status/+",
		topicMetrics: "cluster/metrics/+",
		logger:       e.logger,
	}
	
	// Subscribe to node ready topic to republish jobs when nodes connect
	topicReady := "cluster/nodes/ready"
	if token := client.Subscribe(topicReady, 1, e.handleNodeReady); !token.WaitTimeout(5*time.Second) || token.Error() != nil {
		e.logger.Warnf("Failed to subscribe to node ready topic: %v", token.Error())
	} else {
		e.logger.Infof("Subscribed to node ready topic: %s", topicReady)
	}

	if token := client.Subscribe(m.topicClaims, 1, e.handleClaimMessage); !token.WaitTimeout(5*time.Second) || token.Error() != nil {
		return nil, token.Error()
	}

	if token := client.Subscribe(m.topicCreate, 1, e.handleJobCreate); !token.WaitTimeout(5*time.Second) || token.Error() != nil {
		return nil, token.Error()
	}

	if token := client.Subscribe(m.topicStatus, 1, e.handleTaskStatus); !token.WaitTimeout(5*time.Second) || token.Error() != nil {
		return nil, token.Error()
	}

	if token := client.Subscribe(m.topicMetrics, 1, e.handleMetricsMessage); !token.WaitTimeout(5*time.Second) || token.Error() != nil {
		return nil, token.Error()
	}

	e.logger.Infof("Connected to MQTT broker %s (claims, create, status, metrics, node-ready subscribed)", broker)

	return m, nil
}

// publishJob publishes a new job to MQTT
func (m *EdgeMQTT) publishJob(task shared.Task) {
	payload, _ := json.Marshal(task)
	token := m.client.Publish(m.topicJobs, 0, false, payload)
	if token.Wait() && token.Error() != nil {
		m.logger.Warnf("Failed to publish job %s: %v", task.ID, token.Error())
	}
}

// republishAllPendingJobs republishes all pending jobs to ensure workers see them
func (m *EdgeMQTT) republishAllPendingJobs(cluster *ClusterManager) {
	cluster.mutex.RLock()
	pendingJobs := make([]shared.Task, 0)
	for _, task := range cluster.jobQueue {
		if task.Status == shared.TaskStatusPending {
			pendingJobs = append(pendingJobs, task)
		}
	}
	cluster.mutex.RUnlock()
	
	if len(pendingJobs) > 0 {
		m.logger.Infof("Republishing %d pending jobs", len(pendingJobs))
		for i, task := range pendingJobs {
			m.logger.Infof("Republishing job %s (type=%s, memory=%dMB)", task.ID, task.Name, task.MemoryUsage)
			m.publishJob(task)
			if i < len(pendingJobs)-1 {
				time.Sleep(100 * time.Millisecond) // Longer delay between jobs
			}
		}
		m.logger.Infof("Finished republishing %d pending jobs", len(pendingJobs))
	} else {
		m.logger.Infof("No pending jobs to republish")
	}
}

// publishAssignment sends a job assignment to a specific worker
func (m *EdgeMQTT) publishAssignment(nodeID string, task shared.Task) {
	payload, _ := json.Marshal(task)
	topic := m.topicAssign + nodeID
	token := m.client.Publish(topic, 1, false, payload)
	if token.Wait() && token.Error() != nil {
		m.logger.Warnf("Failed to publish assignment to %s: %v", nodeID, token.Error())
	}
}

// publishJobCreate publishes a job creation request
func (m *EdgeMQTT) publishJobCreate(reqData []byte) {
	m.client.Publish(m.topicCreate, 1, false, reqData)
}

// handleClaimMessage processes worker job claims via MQTT
func (e *EdgeServer) handleClaimMessage(_ mqtt.Client, msg mqtt.Message) {
	var claim struct {
		JobID  string `json:"job_id"`
		NodeID string `json:"node_id"`
	}
	if err := json.Unmarshal(msg.Payload(), &claim); err != nil {
		e.logger.Warnf("Invalid claim message: %v", err)
		return
	}

	e.cluster.mutex.Lock()

	var job *shared.Task
	jobIdx := -1
	for i := range e.cluster.jobQueue {
		if e.cluster.jobQueue[i].ID == claim.JobID && e.cluster.jobQueue[i].Status == shared.TaskStatusPending {
			job = &e.cluster.jobQueue[i]
			jobIdx = i
			break
		}
	}
	if job == nil {
		e.cluster.mutex.Unlock()
		e.logger.Infof("Claim for job %s by %s ignored (not found/pending)", claim.JobID, claim.NodeID)
		return
	}
	
	members := e.serf.Members()
	var claimingMember *serf.Member
	for _, m := range members {
		if m.Name == claim.NodeID && m.Status == serf.StatusAlive {
			mCopy := m
			claimingMember = &mCopy
			break
		}
	}
	if claimingMember == nil {
		e.cluster.mutex.Unlock()
		e.logger.Infof("Claim rejected: node %s not found or not alive", claim.NodeID)
		return
	}
	
	status, hasStatus := claimingMember.Tags["status"]
	// Only reject if status is explicitly "busy", allow if missing/empty (treat as "online")
	if hasStatus && status == "busy" {
		e.cluster.mutex.Unlock()
		e.logger.Infof("Claim rejected: node %s is busy", claim.NodeID)
		return
	}
	
	// Try to get free_memory, fallback to memory_limit if not available
	freeMemoryStr, hasFreeMemory := claimingMember.Tags["free_memory"]
	memoryLimitStr, hasMemoryLimit := claimingMember.Tags["memory_limit"]
	
	var freeMemory int
	if hasFreeMemory {
		var err error
		freeMemory, err = strconv.Atoi(freeMemoryStr)
		if err != nil {
			freeMemory = 0
		}
	} else if hasMemoryLimit {
		// Fallback: use memory_limit as available memory if free_memory not set
		var err error
		freeMemory, err = strconv.Atoi(memoryLimitStr)
		if err != nil {
			freeMemory = 0
		}
	} else {
		// No memory tags at all - allow claim, node will verify itself
		best := (*serf.Member)(nil)
		bestReason := ""
		if e.cluster != nil && e.cluster.scheduler != nil && e.serf != nil {
			best, bestReason = e.cluster.scheduler.BestWorkerForTaskLocked(*job, e.serf.Members())
		}
		if best != nil && claim.NodeID != best.Name {
			e.cluster.mutex.Unlock()
			e.logger.Infof("Claim rejected: job %s assigned to %s (scheduler: %s)", claim.JobID, best.Name, bestReason)
			return
		}

		e.logger.Infof("Claim accepted: node %s (memory tags not set yet, node will verify)", claim.NodeID)
		jobCopy := *job
		// Remove from queue immediately on assignment so it doesn't stay in "waiting" forever.
		if jobIdx >= 0 {
			e.cluster.jobQueue = append(e.cluster.jobQueue[:jobIdx], e.cluster.jobQueue[jobIdx+1:]...)
		}
		if e.cluster != nil && e.cluster.scheduler != nil {
			e.cluster.scheduler.OnAssigned(claim.NodeID, jobCopy)
		}
		eM := e.getMQTT()
		if bestReason != "" {
			e.cluster.logger.Infof("Job %s assigned to %s via MQTT (scheduler: %s)", claim.JobID, claim.NodeID, bestReason)
		} else {
			e.cluster.logger.Infof("Job %s assigned to %s via MQTT", claim.JobID, claim.NodeID)
		}
		e.cluster.mutex.Unlock()
		if eM != nil {
			eM.publishAssignment(claim.NodeID, jobCopy)
		}
		e.broadcastClusterState()
		return
	}
	
	if freeMemory < job.MemoryUsage {
		e.cluster.mutex.Unlock()
		e.logger.Infof("Claim rejected: node %s has insufficient memory (%d < %d)", claim.NodeID, freeMemory, job.MemoryUsage)
		return
	}

	best := (*serf.Member)(nil)
	bestReason := ""
	if e.cluster != nil && e.cluster.scheduler != nil && e.serf != nil {
		best, bestReason = e.cluster.scheduler.BestWorkerForTaskLocked(*job, e.serf.Members())
	}
	if best != nil && claim.NodeID != best.Name {
		e.cluster.mutex.Unlock()
		e.logger.Infof("Claim rejected: job %s assigned to %s (scheduler: %s)", claim.JobID, best.Name, bestReason)
		return
	}

	e.logger.Infof("Claim accepted: node %s claiming job %s (free: %dMB, required: %dMB)", claim.NodeID, claim.JobID, freeMemory, job.MemoryUsage)

	jobCopy := *job
	// Remove from queue immediately on assignment so it doesn't stay in "waiting" forever.
	if jobIdx >= 0 {
		e.cluster.jobQueue = append(e.cluster.jobQueue[:jobIdx], e.cluster.jobQueue[jobIdx+1:]...)
	}
	if e.cluster != nil && e.cluster.scheduler != nil {
		e.cluster.scheduler.OnAssigned(claim.NodeID, jobCopy)
	}
	
	eM := e.getMQTT()
	if bestReason != "" {
		e.cluster.logger.Infof("Job %s assigned to %s via MQTT (scheduler: %s)", claim.JobID, claim.NodeID, bestReason)
	} else {
		e.cluster.logger.Infof("Job %s assigned to %s via MQTT", claim.JobID, claim.NodeID)
	}
	e.cluster.mutex.Unlock()

	if eM != nil {
		eM.publishAssignment(claim.NodeID, jobCopy)
	}

	e.broadcastClusterState()
	
	go func() {
		time.Sleep(300 * time.Millisecond)
		e.broadcastClusterState()
	}()
}

// getMQTT returns the MQTT client instance
func (e *EdgeServer) getMQTT() *EdgeMQTT {
	if v := e.clusterMQTT; v != nil {
		return v
	}
	return nil
}

// attachMQTT attaches the MQTT client to the edge server
func (e *EdgeServer) attachMQTT(m *EdgeMQTT) {
	e.clusterMQTT = m
}

// handleJobCreate processes job creation requests via MQTT
func (e *EdgeServer) handleJobCreate(_ mqtt.Client, msg mqtt.Message) {
	var req shared.JobRequest
	if err := json.Unmarshal(msg.Payload(), &req); err != nil {
		e.logger.Warnf("Invalid job create message: %v", err)
		return
	}

	if req.Type < 1 || req.Type > 4 {
		e.logger.Warnf("Invalid task type: %d", req.Type)
		return
	}

	if req.Duration <= 0 {
		e.logger.Warnf("Invalid duration: %d", req.Duration)
		return
	}
	if req.Duration < 10 {
		req.Duration = 30
	}

	// Default to camera mode if not specified
	inputMode := req.InputMode
	if inputMode == "" {
		inputMode = shared.InputModeCamera
	}

	filePath := req.FilePath
	fileType := req.FileType
	fileURL := req.FileURL
	if inputMode == shared.InputModeFile && fileURL != "" {
		// Reuse the same edge-side prefetch/convert logic as the HTTP API path.
		if apiServer := (&APIServer{cluster: e.cluster, logger: e.logger}); true {
			if newPath, newType, newURL, err := apiServer.ensureEdgeHostedFile(fileURL, filePath, fileType); err != nil {
				e.logger.Warnf("Failed to fetch file URL on edge (job rejected): %v", err)
				return
			} else {
				filePath, fileType, fileURL = newPath, newType, newURL
			}
		}
	}

	taskID := e.cluster.addJob(req.Type, req.Duration, inputMode, filePath, fileType, fileURL)
	e.logger.Infof("Job created via MQTT: %s (type=%d, duration=%d)", taskID, req.Type, req.Duration)

	e.broadcastClusterState()

	if e.clusterMQTT != nil {
		e.clusterMQTT.republishAllPendingJobs(e.cluster)
		e.logger.Infof("Published all pending jobs to MQTT")
	} else {
		e.logger.Warnf("MQTT not available, job created but not published")
	}
}

// handleTaskStatus processes task completion/status updates from workers
func (e *EdgeServer) handleTaskStatus(_ mqtt.Client, msg mqtt.Message) {
	var status struct {
		NodeID string `json:"node_id"`
		TaskID string `json:"task_id"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(msg.Payload(), &status); err != nil {
		e.logger.Warnf("Invalid task status message: %v", err)
		return
	}

	if status.Status == "started" {
		e.logger.Infof("Task %s started on node %s via MQTT", status.TaskID, status.NodeID)
		
		e.cluster.mutex.Lock()
		for i, j := range e.cluster.jobQueue {
			if j.ID == status.TaskID {
				e.cluster.jobQueue = append(e.cluster.jobQueue[:i], e.cluster.jobQueue[i+1:]...)
				break
			}
		}
		e.cluster.mutex.Unlock()
		
		e.broadcastClusterState()
	} else if status.Status == "completed" {
		e.logger.Infof("Task %s completed on node %s via MQTT", status.TaskID, status.NodeID)
		
		e.broadcastClusterState()
		
		go func() {
			time.Sleep(200 * time.Millisecond)
			if e.clusterMQTT != nil {
				e.clusterMQTT.republishAllPendingJobs(e.cluster)
				e.logger.Infof("Republished pending jobs after task completion")
			}
			time.Sleep(100 * time.Millisecond)
			e.broadcastClusterState()
		}()
	}
}

func (e *EdgeServer) handleMetricsMessage(_ mqtt.Client, msg mqtt.Message) {
    var payload struct {
        NodeID      string        `json:"node_id"`
        MemoryLimit int           `json:"memory_limit"`
        FreeMemory  int           `json:"free_memory"`
        Status      string        `json:"status"`
        CurrentTask *shared.Task  `json:"current_task"`
    }
    if err := json.Unmarshal(msg.Payload(), &payload); err != nil {
        e.logger.Warnf("Invalid metrics message: %v", err)
        return
    }

    e.cluster.updateNodeMetrics(payload.NodeID, payload.MemoryLimit, payload.FreeMemory, payload.Status, payload.CurrentTask)
    e.broadcastClusterState()
}

// handleNodeReady processes "node ready" messages from worker nodes
// When a node signals it's ready, we republish all pending jobs
func (e *EdgeServer) handleNodeReady(_ mqtt.Client, msg mqtt.Message) {
	var payload struct {
		NodeID string `json:"node_id"`
	}
	if err := json.Unmarshal(msg.Payload(), &payload); err != nil {
		e.logger.Warnf("Invalid node ready message: %v", err)
		return
	}
	
	e.logger.Infof("Node %s signaled ready via MQTT, republishing pending jobs", payload.NodeID)
	
	// Small delay to ensure node's subscription is fully active
	go func() {
		time.Sleep(200 * time.Millisecond)
		// Republish all pending jobs so the newly ready node can claim them
		if e.clusterMQTT != nil {
			e.clusterMQTT.republishAllPendingJobs(e.cluster)
			e.logger.Infof("Republished pending jobs for node %s", payload.NodeID)
		}
		e.broadcastClusterState()
	}()
}


