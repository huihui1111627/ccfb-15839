package main

// BindMode 存储类的绑定模式
type BindMode string

const (
	BindImmediate       BindMode = "Immediate"            // 立即绑定
	BindWaitForConsumer BindMode = "WaitForFirstConsumer" // 延迟绑定
)

// StorageClass 存储类
type StorageClass struct {
	Name             string
	Mode             BindMode
	DynamicProvision bool // 是否支持动态供给
}

// Node 集群节点
type Node struct {
	Name    string
	Zone    string
	Pool    string // 节点池标签：compute / storage
	CPUCap  int
	MemCap  int // Gi
	CPUUsed int
	MemUsed int
	Dead    bool
}

// Disk 云盘（云厂商侧对象）
type Disk struct {
	ID     string
	Zone   string
	SizeGi int
	State  string // creating / available
	PVName string // 反向引用对应的持久卷
}

// PV 持久卷
type PV struct {
	Name       string
	SC         string
	Zone       string
	CapacityGi int
	AccessMode string
	Phase      string // Available / Reserved / Bound
	ClaimRef   string
	DiskID     string
	Static     bool // 静态预建 or 动态供给
}

// PVC 存储声明
type PVC struct {
	Name             string
	SC               string
	AccessMode       string
	RequestGi        int
	Phase            string // Pending / Assumed / Bound / Resizing
	VolumeName       string
	Zone             string
	AssumedBy        string // 假定该绑定的 Pod
	PodName          string
	ReservedPV       string // 假定时预占的静态卷
	Provisioning     bool   // 是否有供给在进行
	ProvisionSeq     int    // 供给尝试序号，用于识别过期完成事件
	ProvisionRetries int
	Failed           bool
	FailedReason     string
	CreatedAt        float64
	BoundAt          float64
}

// Pod 容器（有状态服务副本）
type Pod struct {
	Name             string
	Service          string
	CPU              int
	Mem              int
	Pool             string // 节点池亲和要求，空表示无要求
	PVCNames         []string
	AntiAffinityZone bool // 同服务副本按可用区反亲和
	NodeName         string
	Zone             string
	Phase            string // Pending / VolumesBinding / Booting / Running
	CreatedAt        float64
	StartedAt        float64
	Reschedules      int
	Generation       int // 重建代数，用于作废过期的启动事件
	AssumeGen        int // 假定代数，用于作废过期的假定超时事件
	LastReason       string
}

// Service 有状态服务
type Service struct {
	Name             string
	Replicas         int
	AntiAffinityZone bool
}

// LogEntry 事件日志
type LogEntry struct {
	Time float64
	Kind string // schedule / bind / provision / orphan / resize / node
	Msg  string
}

// ---------------- 报告快照 ----------------

type PVCSnap struct {
	Name      string
	SC        string
	Mode      string
	Phase     string
	Zone      string
	Volume    string
	Pod       string
	ReqGi     int
	BoundWait float64 // 绑定耗时，-1 表示未绑定
	Failed    bool
	Reason    string
}

type PodSnap struct {
	Name        string
	Service     string
	Phase       string
	Node        string
	Zone        string
	Reason      string
	PVCs        []string
	CPU         int
	Mem         int
	Reschedules int
	TTR         float64 // 创建到运行时长，-1 表示未运行
}

type NodeSnap struct {
	Name    string
	Zone    string
	Pool    string
	CPUCap  int
	CPUUsed int
	MemCap  int
	MemUsed int
	Dead    bool
}

type ZoneSnap struct {
	Name  string
	Quota int
	Used  int
	Pct   float64
}

type ScenarioReport struct {
	Mode           string
	SimDuration    float64
	TotalPods      int
	RunningPods    int
	StuckPods      int
	StuckReasons   map[string]int
	AntiTotal      int
	AntiSatisfied  int
	TTRAvg         float64
	TTRP50         float64
	TTRP95         float64
	TTRMax         float64
	BindAvg        float64
	BindP95        float64
	ProvisionFails int
	AssumeAborts   int
	OrphanReclaims int
	Reschedules    int
	OrphanDisks    int
	OrphanList     []string
	Zones          []ZoneSnap
	QuotaBalance   float64 // 各可用区配额使用率的标准差，越小越均衡
	Nodes          []NodeSnap
	PVCs           []PVCSnap
	Pods           []PodSnap
	Events         []LogEntry
}

type Experiment struct {
	Seed      int64
	Immediate *ScenarioReport
	Delayed   *ScenarioReport
}
