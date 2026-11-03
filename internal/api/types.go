// Package api defines the resource contracts shared by the control plane.
package api

import "time"

const Version = "mini.k8s/v1"

type Metadata struct {
	Name              string            `json:"name"`
	Namespace         string            `json:"namespace,omitempty"`
	UID               string            `json:"uid,omitempty"`
	ResourceVersion   int64             `json:"resourceVersion,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	Annotations       map[string]string `json:"annotations,omitempty"`
	OwnerUID          string            `json:"ownerUID,omitempty"`
	DeletionTimestamp *time.Time        `json:"deletionTimestamp,omitempty"`
}

// Quantities are expressed as millicores and bytes, respectively.
type Resources struct {
	CPUMilli    int64 `json:"cpuMilli"`
	MemoryBytes int64 `json:"memoryBytes"`
}

type ResourceRequirements struct {
	Requests Resources `json:"requests"`
	Limits   Resources `json:"limits"`
}

type TCPProbe struct {
	Port int `json:"port"`
}

type PodSpec struct {
	Image         string               `json:"image"`
	Command       []string             `json:"command,omitempty"`
	Args          []string             `json:"args,omitempty"`
	Env           map[string]string    `json:"env,omitempty"`
	Resources     ResourceRequirements `json:"resources"`
	RestartPolicy string               `json:"restartPolicy"`
	Readiness     *TCPProbe            `json:"readiness,omitempty"`
}

type PodStatus struct {
	Phase        string `json:"phase,omitempty"`
	Reason       string `json:"reason,omitempty"`
	Ready        bool   `json:"ready,omitempty"`
	PodIP        string `json:"podIP,omitempty"`
	RestartCount int64  `json:"restartCount,omitempty"`
}

type Pod struct {
	APIVersion string    `json:"apiVersion"`
	Kind       string    `json:"kind"`
	Metadata   Metadata  `json:"metadata"`
	Spec       PodSpec   `json:"spec"`
	NodeUID    string    `json:"nodeUID,omitempty"`
	Status     PodStatus `json:"status,omitempty"`
}

type PodTemplate struct {
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations,omitempty"`
	Spec        PodSpec           `json:"spec"`
}

type ReplicaSetSpec struct {
	Replicas int               `json:"replicas"`
	Selector map[string]string `json:"selector"`
	Template PodTemplate       `json:"template"`
}

type ReplicaSetStatus struct {
	Replicas      int `json:"replicas,omitempty"`
	ReadyReplicas int `json:"readyReplicas,omitempty"`
}

type ReplicaSet struct {
	APIVersion string           `json:"apiVersion"`
	Kind       string           `json:"kind"`
	Metadata   Metadata         `json:"metadata"`
	Spec       ReplicaSetSpec   `json:"spec"`
	Status     ReplicaSetStatus `json:"status,omitempty"`
}

type NodeSpec struct {
	Allocatable   Resources `json:"allocatable"`
	PodCIDR       string    `json:"podCIDR"`
	Unschedulable bool      `json:"unschedulable,omitempty"`
}

type NodeStatus struct {
	Ready         bool      `json:"ready"`
	LastHeartbeat time.Time `json:"lastHeartbeat,omitempty"`
	Reserved      Resources `json:"reserved"`
}

type Node struct {
	APIVersion string     `json:"apiVersion"`
	Kind       string     `json:"kind"`
	Metadata   Metadata   `json:"metadata"`
	Spec       NodeSpec   `json:"spec"`
	Status     NodeStatus `json:"status,omitempty"`
}

type ServiceSpec struct {
	Selector   map[string]string `json:"selector"`
	Port       int               `json:"port"`
	TargetPort int               `json:"targetPort"`
	ClusterIP  string            `json:"clusterIP,omitempty"`
}

type Service struct {
	APIVersion string      `json:"apiVersion"`
	Kind       string      `json:"kind"`
	Metadata   Metadata    `json:"metadata"`
	Spec       ServiceSpec `json:"spec"`
}
