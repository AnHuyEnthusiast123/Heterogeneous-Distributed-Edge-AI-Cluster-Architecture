//go:build !mqtt
// +build !mqtt

package main

import (
	"cluster-docker/shared"
	"errors"
)

type NodeMQTT struct{}

// initMQTT returns an error when MQTT is not enabled
func (m *MilkvNode) initMQTT() (*NodeMQTT, error) {
	return nil, errors.New("mqtt build tag not enabled")
}

// publishCompletion is a no-op when MQTT is not enabled
func (n *NodeMQTT) publishStatus(taskID string, status string) {
	_ = taskID
	_ = status
}

func (n *NodeMQTT) publishCompletion(task *shared.Task, status string) {
	_ = task
	_ = status
}

func (n *NodeMQTT) publishMetrics(totalMB, freeMB int, status string, taskID, taskName, inputMode string) {
	_ = totalMB
	_ = freeMB
	_ = status
	_ = taskID
	_ = taskName
	_ = inputMode
}


