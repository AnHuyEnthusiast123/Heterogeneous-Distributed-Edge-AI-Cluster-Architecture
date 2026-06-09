package shared

import (
	"time"
)

// TaskType represents the type of task
type TaskType int

const (
	TaskDrowsiness              TaskType = 1  // drowsiness_detection
	TaskHeatmap                 TaskType = 2  // thermal_heatmap
	TaskFaceDetection           TaskType = 3  // face_detection
	TaskPeopleCounting          TaskType = 4  // people_counting
	TaskObjectTracking          TaskType = 5  // object_tracking
	TaskLicensePlateRecognition TaskType = 6  // license_plate_recognition
	TaskGestureRecognition      TaskType = 7  // gesture_recognition
	TaskAnomalyDetection        TaskType = 8  // anomaly_detection
	TaskVehicleCounting         TaskType = 9  // vehicle_counting
	TaskCrowdDensityAnalysis    TaskType = 10 // crowd_density_analysis
)

// InputMode represents the input source for a task
type InputMode string

const (
	InputModeCamera InputMode = "camera"
	InputModeFile   InputMode = "file"
)

// Task represents a job task
type Task struct {
	ID          string     `json:"id"`
	Type        TaskType   `json:"type"`
	Name        string     `json:"name"`
	MemoryUsage int        `json:"memory_usage"` // in MB
	Duration    int        `json:"duration"`     // in seconds
	Status      TaskStatus `json:"status"`
	CreatedAt   time.Time  `json:"created_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`

	// Optional per-task config + derived scheduling features (compatible with PSO-ACO-GA_perTask.go naming)
	ConfigPath           string  `json:"config_path,omitempty"`            // relative path under edge FILE_STORAGE_DIR
	ConfigURL            string  `json:"config_url,omitempty"`             // relative URL under /api/files/...
	Complexity           float64 `json:"complexity,omitempty"`             // 0-100 (higher = more complex)
	CPUReq               float64 `json:"cpu_req,omitempty"`                // 0-100 (percent points)
	MemReq               float64 `json:"mem_req,omitempty"`                // MB (may override MemoryUsage)
	EstimatedProcessTime float64 `json:"estimated_process_time,omitempty"` // seconds
	PreTime              float64 `json:"pre_time,omitempty"`               // seconds (batching/preprocessing proxy)
	PowerFactor          float64 `json:"power_factor,omitempty"`           // optional (not currently used by workers)
	DataSizeBits         float64 `json:"data_size_bits,omitempty"`         // input data size in bits (for computation intensity; 0 = derive from MemoryUsage)
	// File-based task fields
	InputMode InputMode `json:"input_mode,omitempty"` // "camera" or "file"
	FilePath  string    `json:"file_path,omitempty"`  // Local file path or API URL
	FileType  string    `json:"file_type,omitempty"`  // "mp4" or "h264"
	FileURL   string    `json:"file_url,omitempty"`   // Full URL to download file from API
}

// TaskStatus represents the status of a task
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusStopped   TaskStatus = "stopped"
)

// Node represents a cluster node
type Node struct {
	ID          string     `json:"id"`
	Type        string     `json:"type"`         // "edge" or "milkv"
	MemoryLimit int        `json:"memory_limit"` // in MB
	FreeMemory  int        `json:"free_memory"`  // in MB
	CurrentTask *Task      `json:"current_task,omitempty"`
	TaskQueue   []Task     `json:"task_queue"`
	LastSeen    time.Time  `json:"last_seen"`
	Status      NodeStatus `json:"status"`
	IPAddress   string     `json:"ip_address,omitempty"` // Node IP address for RTSP
}

// NodeStatus represents the status of a node
type NodeStatus string

const (
	NodeStatusOnline  NodeStatus = "online"
	NodeStatusOffline NodeStatus = "offline"
	NodeStatusBusy    NodeStatus = "busy"
)

// ClusterState represents the overall cluster state
type ClusterState struct {
	Nodes      map[string]*Node `json:"nodes"`
	JobQueue   []Task           `json:"jobQueue"`
	TotalJobs  int              `json:"total_jobs"`
	ActiveJobs int              `json:"active_jobs"`
	LastUpdate time.Time        `json:"last_update"`
}

// JobRequest represents a request to create a new job
type JobRequest struct {
	Type      TaskType  `json:"type"`
	Duration  int       `json:"duration"`
	InputMode InputMode `json:"input_mode,omitempty"` // "camera" or "file"
	FilePath  string    `json:"file_path,omitempty"`  // Local file path or API URL
	FileType  string    `json:"file_type,omitempty"`  // "mp4" or "h264"
	FileURL   string    `json:"file_url,omitempty"`   // Full URL to download file from API

	// Optional per-task config JSON (edge will fetch + parse it and attach derived fields to the task).
	ConfigPath string `json:"config_path,omitempty"` // filename hint
	ConfigURL  string `json:"config_url,omitempty"`  // URL or /api/files/... path
}

// JobResponse represents the response to a job request
type JobResponse struct {
	TaskID   string `json:"task_id"`
	Assigned bool   `json:"assigned"`
	NodeID   string `json:"node_id,omitempty"`
	Message  string `json:"message"`
}

// GetTaskInfo returns information about a task type
func GetTaskInfo(taskType TaskType) (string, int) {
	switch taskType {
	case TaskDrowsiness:
		return "Drowsiness Detection", 58
	case TaskHeatmap:
		return "Thermal Heatmap", 65
	case TaskFaceDetection:
		return "Face Detection", 62
	case TaskPeopleCounting:
		return "People Counting", 70
	case TaskObjectTracking:
		return "Object Tracking", 75
	case TaskLicensePlateRecognition:
		return "License Plate Recognition", 68
	case TaskGestureRecognition:
		return "Gesture Recognition", 72
	case TaskAnomalyDetection:
		return "Anomaly Detection", 80
	case TaskVehicleCounting:
		return "Vehicle Counting", 74
	case TaskCrowdDensityAnalysis:
		return "Crowd Density Analysis", 78
	default:
		return "Unknown Task", 50
	}
}

// GetTaskDisplayName returns a display name for a task including input mode
func GetTaskDisplayName(task *Task) string {
	name := task.Name
	if task.InputMode == InputModeFile {
		if task.FileType != "" {
			name += " (File: " + task.FileType + ")"
		} else {
			name += " (File)"
		}
	} else {
		name += " (Camera)"
	}
	return name
}
