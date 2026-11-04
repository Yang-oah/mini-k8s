// Package api 定义控制面组件共享的资源模型与校验规则。
package api

import "time"

// Version 是此项目资源使用的 API 版本，不代表 Kubernetes API 兼容。
const Version = "mini.k8s/v1"

// Metadata 保存资源标识和元数据。UID、版本、所有者及删除时间由系统管理；
// 用户可声明标签和注解，其中 Pod 创建后标签不可更改。
type Metadata struct {
	Name              string            `json:"name"`
	Namespace         string            `json:"namespace,omitempty"`
	UID               string            `json:"uid,omitempty"`
	ResourceVersion   int64             `json:"resourceVersion,omitempty"` // etcd 修改版本，用于乐观并发控制
	Labels            map[string]string `json:"labels,omitempty"`
	Annotations       map[string]string `json:"annotations,omitempty"`
	OwnerUID          string            `json:"ownerUID,omitempty"`
	DeletionTimestamp *time.Time        `json:"deletionTimestamp,omitempty"`
}

// Resources 以毫核表示 CPU，以字节表示内存。
type Resources struct {
	CPUMilli    int64 `json:"cpuMilli"`
	MemoryBytes int64 `json:"memoryBytes"`
}

// ResourceRequirements 分别描述调度预留的请求量和容器运行时限制。
type ResourceRequirements struct {
	Requests Resources `json:"requests"`
	Limits   Resources `json:"limits"`
}

// TCPProbe 描述就绪探测使用的 TCP 端口。
type TCPProbe struct {
	Port int `json:"port"`
}

// PodSpec 是单容器 Pod 的用户期望；创建后整体不可原地修改。
type PodSpec struct {
	Image         string               `json:"image"`
	Command       []string             `json:"command,omitempty"`
	Args          []string             `json:"args,omitempty"`
	Env           map[string]string    `json:"env,omitempty"`
	Resources     ResourceRequirements `json:"resources"`
	RestartPolicy string               `json:"restartPolicy"`
	Readiness     *TCPProbe            `json:"readiness,omitempty"`
}

// PodStatus 保存系统观察到的运行状态，不属于用户声明。
type PodStatus struct {
	Phase  string `json:"phase,omitempty"`
	Reason string `json:"reason,omitempty"`
	Ready  bool   `json:"ready,omitempty"`
	// ReadinessEpoch 将本次成功探针与节点的就绪代次绑定；不是完整的节点会话栅栏。
	ReadinessEpoch uint64 `json:"readinessEpoch,omitempty"`
	PodIP          string `json:"podIP,omitempty"`
	RestartCount   int64  `json:"restartCount,omitempty"`
}

// Pod 将用户声明、系统绑定和实际状态分开保存。
type Pod struct {
	APIVersion string    `json:"apiVersion"`
	Kind       string    `json:"kind"`
	Metadata   Metadata  `json:"metadata"`
	Spec       PodSpec   `json:"spec"`
	NodeUID    string    `json:"nodeUID,omitempty"` // 由绑定操作写入，用户不可修改
	Status     PodStatus `json:"status,omitempty"`
}

// PodTemplate 是 ReplicaSet 创建子 Pod 时使用的模板。
type PodTemplate struct {
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations,omitempty"`
	Spec        PodSpec           `json:"spec"`
}

// ReplicaSetSpec 描述副本数、选择器和模板；创建后只允许修改副本数。
type ReplicaSetSpec struct {
	Replicas int               `json:"replicas"`
	Selector map[string]string `json:"selector"`
	Template PodTemplate       `json:"template"`
}

// ReplicaSetStatus 保存控制面观察到的副本状态。
type ReplicaSetStatus struct {
	Replicas      int `json:"replicas,omitempty"`
	ReadyReplicas int `json:"readyReplicas,omitempty"`
}

// ReplicaSet 表示需要维持指定数量 Pod 的工作负载。
type ReplicaSet struct {
	APIVersion string           `json:"apiVersion"`
	Kind       string           `json:"kind"`
	Metadata   Metadata         `json:"metadata"`
	Spec       ReplicaSetSpec   `json:"spec"`
	Status     ReplicaSetStatus `json:"status,omitempty"`
}

// NodeSpec 保存节点可分配资源、Pod 网段和调度开关。
type NodeSpec struct {
	Allocatable   Resources `json:"allocatable"`
	PodCIDR       string    `json:"podCIDR"`
	Unschedulable bool      `json:"unschedulable,omitempty"`
}

// NodeStatus 保存节点就绪状态、心跳和系统维护的已预留资源。
type NodeStatus struct {
	Ready     bool   `json:"ready"`
	Condition string `json:"condition,omitempty"`
	// ReadinessEpoch 由控制面维护，失联或重新就绪时改变，旧探针结果随之失效。
	ReadinessEpoch uint64    `json:"readinessEpoch,omitempty"`
	LastHeartbeat  time.Time `json:"lastHeartbeat,omitempty"`
	Reserved       Resources `json:"reserved"`
}

// Node 表示由控制面管理、Agent 上报状态的节点。
type Node struct {
	APIVersion string     `json:"apiVersion"`
	Kind       string     `json:"kind"`
	Metadata   Metadata   `json:"metadata"`
	Spec       NodeSpec   `json:"spec"`
	Status     NodeStatus `json:"status,omitempty"`
}

// ServiceSpec 预留 Service 的选择器和单端口配置；ClusterIP 由系统管理。
type ServiceSpec struct {
	Selector   map[string]string `json:"selector"`
	Port       int               `json:"port"`
	TargetPort int               `json:"targetPort"`
	ClusterIP  string            `json:"clusterIP,omitempty"`
}

// Service 表示稳定入口的资源模型；转发能力由后续阶段实现。
type Service struct {
	APIVersion string      `json:"apiVersion"`
	Kind       string      `json:"kind"`
	Metadata   Metadata    `json:"metadata"`
	Spec       ServiceSpec `json:"spec"`
}
