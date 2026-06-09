//go:build mqtt
// +build mqtt

package main

import (
	"bytes"
	"encoding/json"
	"net/url"
	"os"
	"time"

	"cluster-docker/shared"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type NodeMQTT struct {
	client      mqtt.Client
	topicJobs   string
	topicClaims string
	topicAssign string
	topicMetrics string
	node        *MilkvNode
}

// initMQTT initializes the MQTT client for the worker node
func (m *MilkvNode) initMQTT() (*NodeMQTT, error) {
	broker := os.Getenv("MQTT_BROKER")
	if broker == "" {
		if u, err := url.Parse(m.edgeAPIBase); err == nil && u.Hostname() != "" {
			broker = "tcp://" + u.Hostname() + ":1883"
		} else {
			broker = "tcp://localhost:1883"
		}
	}
	clientID := m.nodeID
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

	n := &NodeMQTT{
		client:       client,
		topicJobs:    "cluster/jobs",
		topicClaims:  "cluster/claims",
		topicAssign:  "cluster/assign/" + m.nodeID,
		topicMetrics: "cluster/metrics/" + m.nodeID,
		node:         m,
	}

	if token := client.Subscribe(n.topicAssign, 1, n.onAssignment); !token.WaitTimeout(5*time.Second) || token.Error() != nil {
		return nil, token.Error()
	}
	m.logger.Infof("Connected to MQTT broker %s (assign subscribed)", broker)

	if token := client.Subscribe(n.topicJobs, 0, n.onJobIncoming); !token.WaitTimeout(5*time.Second) || token.Error() != nil {
		return nil, token.Error()
	}
	m.logger.Infof("Subscribed to job topic: %s", n.topicJobs)

	// Signal to edge server that this node is ready to receive jobs
	go func() {
		// Longer delay to ensure subscription is fully active and MQTT broker has processed it
		time.Sleep(1 * time.Second)
		n.signalNodeReady()
	}()

	return n, nil
}

// onJobIncoming handles incoming job announcements from MQTT
func (n *NodeMQTT) onJobIncoming(_ mqtt.Client, msg mqtt.Message) {
	if n.node.currentTask != nil {
		n.node.logger.Debugf("Received job but already have task, ignoring")
		return
	}
	var job shared.Task
	if err := json.Unmarshal(msg.Payload(), &job); err != nil {
		n.node.logger.Warnf("Failed to unmarshal job: %v", err)
		return
	}
	n.node.logger.Infof("Received job announcement: %s (type=%s, memory=%dMB, status=%s)", job.ID, job.Name, job.MemoryUsage, job.Status)
	if job.Status != shared.TaskStatusPending {
		n.node.logger.Debugf("Job %s status is %s, not pending, ignoring", job.ID, job.Status)
		return
	}
	if job.CreatedAt.IsZero() {
		n.node.logger.Warnf("Job %s has zero CreatedAt timestamp, ignoring", job.ID)
		return
	}
	if time.Since(job.CreatedAt) > 10*time.Minute {
		n.node.logger.Infof("Ignoring very old job %s (created %v ago)", job.ID, time.Since(job.CreatedAt))
		return
	}
	_, availMB := getSystemMemoryMB(n.node.logger, n.node.memoryLimit)
	available := availMB
	if job.MemoryUsage <= available {
		claim := map[string]string{
			"job_id":  job.ID,
			"node_id": n.node.nodeID,
		}
		b, _ := json.Marshal(claim)
		n.node.logger.Infof("Claiming job %s via MQTT (available: %dMB, required: %dMB)", job.ID, available, job.MemoryUsage)
		n.client.Publish(n.topicClaims, 1, false, b)
	} else {
		n.node.logger.Infof("Job %s requires %dMB but only %dMB available", job.ID, job.MemoryUsage, available)
	}
}

// onAssignment handles job assignment messages from the edge server
func (n *NodeMQTT) onAssignment(_ mqtt.Client, msg mqtt.Message) {
	var task shared.Task
	if err := json.Unmarshal(msg.Payload(), &task); err != nil {
		n.node.logger.Warnf("Failed to unmarshal assignment: %v", err)
		return
	}
	n.node.logger.Infof("Received job assignment: %s (%s, %dMB, %ds)", task.ID, task.Name, task.MemoryUsage, task.Duration)
	
	taskCopy := task
	n.node.currentTask = &taskCopy
	
	n.node.updateMemoryTags()
}

// publishStatus publishes task status updates via MQTT
func (n *NodeMQTT) publishStatus(taskID string, status string) {
	evt := map[string]any{
		"node_id": n.node.nodeID,
		"task_id": taskID,
		"status":  status,
	}
	b, _ := json.Marshal(evt)
	n.client.Publish("cluster/status/"+n.node.nodeID, 1, false, bytes.NewBuffer(b).Bytes())
}

func (n *NodeMQTT) publishMetrics(totalMB, freeMB int, status string, taskID, taskName, inputMode string) {
	// Use QoS 0 for metrics to avoid building up in-memory publish queues.
	evt := map[string]any{
		"node_id":      n.node.nodeID,
		"memory_limit": totalMB,
		"free_memory":  freeMB,
		"status":       status,
		"timestamp":    time.Now().UnixNano(),
	}
	if taskID != "" {
		evt["current_task_id"] = taskID
		evt["current_task_name"] = taskName
		evt["current_task_input_mode"] = inputMode
	}
	b, _ := json.Marshal(evt)
	n.client.Publish(n.topicMetrics, 0, false, b)
}

// publishCompletion publishes task final status via MQTT
func (n *NodeMQTT) publishCompletion(task *shared.Task, status string) {
	n.publishStatus(task.ID, status)
}

// signalNodeReady signals to the edge server that this node is ready to receive jobs
// This triggers the edge server to republish all pending jobs
func (n *NodeMQTT) signalNodeReady() {
	topicReady := "cluster/nodes/ready"
	payload := map[string]string{
		"node_id": n.node.nodeID,
	}
	b, _ := json.Marshal(payload)
	token := n.client.Publish(topicReady, 1, false, b)
	if token.Wait() && token.Error() != nil {
		n.node.logger.Warnf("Failed to signal node ready: %v", token.Error())
	} else {
		n.node.logger.Infof("Signaled node ready to edge server")
	}
}


