//go:build !mqtt
// +build !mqtt

package main

import (
	"cluster-docker/shared"
	"errors"
)

// EdgeMQTT is a no-op placeholder when built without -tags mqtt
type EdgeMQTT struct{}

func (e *EdgeServer) initMQTT() (*EdgeMQTT, error) {
	return nil, errors.New("mqtt build tag not enabled")
}
func (e *EdgeServer) attachMQTT(_ *EdgeMQTT) {}

func (m *EdgeMQTT) publishAssignment(node string, task shared.Task) {
	_ = node
	_ = task
}

// publishJobCreate is a no-op when MQTT is not enabled
func (m *EdgeMQTT) publishJobCreate(reqData []byte) {
	_ = reqData
}

func (m *EdgeMQTT) republishAllPendingJobs(_ *ClusterManager) {}

var _ = (*ClusterManager).addJob
var _ = (*ClusterManager).updateNodeMetrics
var _ = (*EdgeServer).findBestWorkerForJob
var _ = (*EdgeMQTT).publishAssignment
var _ = (*EdgeMQTT).republishAllPendingJobs

