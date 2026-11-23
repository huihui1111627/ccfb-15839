package main

import (
	"fmt"
	"math/rand"
	"sort"
)

const (
	assumeTimeout       = 30.0 // 调度器卷绑定假定的超时时间（秒）
	maxProvisionRetries = 6    // 立即绑定模式供给失败的最大重试次数
	maxReschedules      = 8    // Pod 最大重调度次数
)

// Cluster 仿真集群：同时承载绑定/供给控制器与调度器的状态
type Cluster struct {
	sim      *Sim
	rnd      *rand.Rand
	cloud    *Cloud
	mode     string // "immediate" / "delayed"（实验标签）
	zoneList []string

	nodes     map[string]*Node
	nodeOrder []string
	scs       map[string]*StorageClass
	pvs       map[string]*PV
	pvcs      map[string]*PVC
	pvcOrder  []string
	pods      map[string]*Pod
	podOrder  []string
	services  map[string]*Service
	svcOrder  []string

	logs []LogEntry

	provisionFails int
	assumeAborts   int
	orphanReclaims int
	reschedules    int
}

func NewCluster(mode string, seed int64) *Cluster {
	sim := NewSim()
	c := &Cluster{
		sim:      sim,
		rnd:      rand.New(rand.NewSource(seed*31 + 7)),
		mode:     mode,
		zoneList: []string{"zone-a", "zone-b", "zone-c"},
		nodes:    map[string]*Node{},
		scs:      map[string]*StorageClass{},
		pvs:      map[string]*PV{},
		pvcs:     map[string]*PVC{},
		pods:     map[string]*Pod{},
		services: map[string]*Service{},
	}
	c.cloud = &Cloud{
		sim:      sim,
		rnd:      rand.New(rand.NewSource(seed*17 + 3)),
		quota:    map[string]int{},
		used:     map[string]int{},
		disks:    map[string]*Disk{},
		failRate: 0.12,
	}
	return c
}

func (c *Cluster) logf(kind, format string, args ...any) {
	c.logs = append(c.logs, LogEntry{Time: c.sim.Now, Kind: kind, Msg: fmt.Sprintf(format, args...)})
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func intersectZones(a, b map[string]bool) map[string]bool {
	out := map[string]bool{}
	for z := range a {
		if b[z] {
			out[z] = true
		}
	}
	return out
}

// ---------------- 声明创建与立即绑定 ----------------

func (c *Cluster) createPVC(name, scName string, size int, podName string) {
	pvc := &PVC{
		Name: name, SC: scName, AccessMode: "RWO", RequestGi: size,
		Phase: "Pending", PodName: podName, CreatedAt: c.sim.Now, BoundAt: -1,
	}
	c.pvcs[name] = pvc
	c.pvcOrder = append(c.pvcOrder, name)
	c.logf("bind", "创建存储声明 %s（存储类 %s，%dGi，绑定模式 %s）", name, scName, size, c.scs[scName].Mode)
	if c.scs[scName].Mode == BindImmediate {
		c.tryBindImmediate(pvc)
	}
}

func (c *Cluster) addPod(p *Pod) {
	p.Phase = "Pending"
	p.CreatedAt = c.sim.Now
	c.pods[p.Name] = p
	c.podOrder = append(c.podOrder, p.Name)
	c.logf("schedule", "Pod %s（服务 %s，cpu=%d，mem=%dGi，节点池=%q，卷=%v）进入调度队列",
		p.Name, p.Service, p.CPU, p.Mem, p.Pool, p.PVCNames)
}

// tryBindImmediate 立即绑定：先在全部可用区里匹配空闲卷（不看拓扑），
// 匹配不到就在随机可用区动态供给 —— 这正是可用区错配的来源。
func (c *Cluster) tryBindImmediate(pvc *PVC) {
	if pvc.Phase != "Pending" || pvc.Failed || pvc.Provisioning {
		return
	}
	if pv := c.findAvailablePV(pvc, ""); pv != nil {
		c.bind(pvc, pv)
		return
	}
	sc := c.scs[pvc.SC]
	if !sc.DynamicProvision {
		c.logf("bind", "声明 %s 无匹配的空闲卷且存储类不支持动态供给，保持未绑定", pvc.Name)
		return
	}
	zone := c.zoneList[c.rnd.Intn(len(c.zoneList))]
	c.logf("provision", "立即绑定：为声明 %s 在随机可用区 %s 动态供给 %dGi 云盘", pvc.Name, zone, pvc.RequestGi)
	c.provisionForPVC(pvc, zone)
}

// findAvailablePV 匹配空闲卷：存储类、访问模式、容量满足；zone 为空则不限制可用区。
// 优先容量最接近的卷。
func (c *Cluster) findAvailablePV(pvc *PVC, zone string) *PV {
	var best *PV
	for _, pv := range c.pvs {
		if pv.Phase != "Available" || pv.SC != pvc.SC || pv.AccessMode != pvc.AccessMode {
			continue
		}
		if pv.CapacityGi < pvc.RequestGi {
			continue
		}
		if zone != "" && pv.Zone != zone {
			continue
		}
		if best == nil || pv.CapacityGi < best.CapacityGi ||
			(pv.CapacityGi == best.CapacityGi && pv.Name < best.Name) {
			best = pv
		}
	}
	return best
}

// ---------------- 动态供给 ----------------

func (c *Cluster) provisionForPVC(pvc *PVC, zone string) {
	pvc.ProvisionSeq++
	seq := pvc.ProvisionSeq
	pvc.Provisioning = true
	c.cloud.Provision(zone, pvc.RequestGi, func(d *Disk, err error) {
		if seq != pvc.ProvisionSeq {
			// 假定已被撤销或该尝试已被更新的尝试取代：
			// 供给出来的盘必须立刻回收，保证不产生孤立盘。
			if d != nil {
				c.cloud.Delete(d.ID)
				c.orphanReclaims++
				c.logf("orphan", "假定已撤销，回收供给完成的云盘 %s（%s/%dGi），避免孤立盘", d.ID, d.Zone, d.SizeGi)
			}
			return
		}
		pvc.Provisioning = false
		c.onProvisionComplete(pvc, d, err)
	})
}

func (c *Cluster) onProvisionComplete(pvc *PVC, d *Disk, err error) {
	if err != nil {
		c.provisionFails++
		if pvc.AssumedBy != "" {
			// 延迟绑定：供给失败 → 撤销调度器的假定，Pod 重新排队
			c.logf("provision", "声明 %s 在 %s 供给失败：%v，撤销 Pod %s 的调度假定",
				pvc.Name, pvc.Zone, err, pvc.AssumedBy)
			c.abortAssume(c.pods[pvc.AssumedBy], err.Error())
			return
		}
		// 立即绑定：退避重试，重试耗尽后声明保持未绑定
		pvc.ProvisionRetries++
		if pvc.ProvisionRetries >= maxProvisionRetries {
			pvc.Failed = true
			pvc.FailedReason = err.Error()
			c.logf("provision", "声明 %s 供给重试 %d 次仍失败（%v），声明保持未绑定", pvc.Name, pvc.ProvisionRetries, err)
			return
		}
		backoff := float64(int(2) << pvc.ProvisionRetries)
		c.logf("provision", "声明 %s 供给失败（%v），%.0fs 后第 %d 次重试", pvc.Name, err, backoff, pvc.ProvisionRetries+1)
		c.sim.After(backoff, func() { c.tryBindImmediate(pvc) })
		return
	}
	// 供给成功：登记持久卷并绑定
	pv := &PV{
		Name: fmt.Sprintf("pv-%s", d.ID), SC: pvc.SC, Zone: d.Zone,
		CapacityGi: d.SizeGi, AccessMode: pvc.AccessMode, Phase: "Available", DiskID: d.ID,
	}
	d.PVName = pv.Name
	c.pvs[pv.Name] = pv
	c.logf("provision", "云盘 %s 供给完成（%s/%dGi）→ 持久卷 %s", d.ID, d.Zone, d.SizeGi, pv.Name)
	c.bind(pvc, pv)
}

// ---------------- 绑定 ----------------

func (c *Cluster) bind(pvc *PVC, pv *PV) {
	pvc.Phase = "Bound"
	pvc.VolumeName = pv.Name
	pvc.Zone = pv.Zone
	pvc.BoundAt = c.sim.Now
	pv.Phase = "Bound"
	pv.ClaimRef = pvc.Name
	c.logf("bind", "声明 %s 绑定到 %s（%s，%dGi）", pvc.Name, pv.Name, pv.Zone, pv.CapacityGi)
	if pvc.PodName != "" {
		if p := c.pods[pvc.PodName]; p != nil {
			c.maybeFinishBind(p)
		}
	}
	c.schedulePending()
}

// maybeFinishBind 调度器假定后：把预占的静态卷落实绑定；全部卷就绪则启动容器。
func (c *Cluster) maybeFinishBind(p *Pod) {
	if p.Phase != "VolumesBinding" {
		return
	}
	for _, pn := range p.PVCNames {
		pvc := c.pvcs[pn]
		if pvc.Phase == "Assumed" && pvc.ReservedPV != "" {
			pv := c.pvs[pvc.ReservedPV]
			pvc.ReservedPV = ""
			c.bind(pvc, pv)
		}
	}
	if p.Phase != "VolumesBinding" {
		return
	}
	for _, pn := range p.PVCNames {
		if c.pvcs[pn].Phase != "Bound" {
			return // 还有卷在供给中
		}
	}
	p.Phase = "Booting"
	gen := p.Generation
	c.sim.After(1+c.rnd.Float64()*2, func() {
		if p.Phase == "Booting" && p.Generation == gen {
			p.Phase = "Running"
			if p.StartedAt < 0 {
				p.StartedAt = c.sim.Now
			}
			c.logf("schedule", "Pod %s 在节点 %s（%s）启动运行", p.Name, p.NodeName, p.Zone)
		}
	})
}

// abortAssume 撤销调度器的卷绑定假定：释放预占的静态卷、标记在途供给作废、
// 释放节点资源，Pod 重新排队。在途供给完成时会被回收（见 provisionForPVC），
// 因此不会留下孤立盘。
func (c *Cluster) abortAssume(p *Pod, reason string) {
	if p == nil || p.Phase != "VolumesBinding" {
		return
	}
	c.assumeAborts++
	p.AssumeGen++
	for _, pn := range p.PVCNames {
		pvc := c.pvcs[pn]
		if pvc.Phase != "Assumed" {
			continue
		}
		if pvc.ReservedPV != "" {
			pv := c.pvs[pvc.ReservedPV]
			pv.Phase = "Available"
			pv.ClaimRef = ""
			pvc.ReservedPV = ""
			c.logf("bind", "撤销假定：静态卷 %s 恢复为空闲", pv.Name)
		}
		if pvc.Provisioning {
			// 在途供给通过 ProvisionSeq 作废，完成时回收云盘
			pvc.ProvisionSeq++
			pvc.Provisioning = false
		}
		pvc.Phase = "Pending"
		pvc.Zone = ""
		pvc.AssumedBy = ""
	}
	if n := c.nodes[p.NodeName]; n != nil && !n.Dead {
		n.CPUUsed -= p.CPU
		n.MemUsed -= p.Mem
	}
	c.logf("schedule", "撤销 Pod %s 的调度假定（%s），重新排队", p.Name, reason)
	p.NodeName = ""
	p.Zone = ""
	p.Phase = "Pending"
	p.Reschedules++
	c.reschedules++
	if p.Reschedules <= maxReschedules {
		c.sim.After(2, func() { c.schedulePending() })
	} else {
		p.LastReason = "重调度次数超过上限：" + reason
	}
}

// ---------------- 扩容 ----------------

func (c *Cluster) resizePVC(pvc *PVC, newSize int) {
	if pvc.Phase != "Bound" {
		return
	}
	pv := c.pvs[pvc.VolumeName]
	old := pv.CapacityGi
	pvc.Phase = "Resizing"
	c.logf("resize", "扩容声明 %s 的卷 %s（%s）：%dGi → %dGi", pvc.Name, pv.Name, pv.Zone, old, newSize)
	c.cloud.Resize(pv.DiskID, newSize, func(err error) {
		if err != nil {
			pvc.Phase = "Bound"
			c.logf("resize", "卷 %s 扩容失败：%v，保持 %dGi", pv.Name, err, old)
			return
		}
		pv.CapacityGi = newSize
		pvc.RequestGi = newSize
		pvc.Phase = "Bound"
		c.logf("resize", "卷 %s 扩容完成：%dGi → %dGi（%s 配额相应扣减）", pv.Name, old, newSize, pv.Zone)
	})
}

// ---------------- 节点删除 ----------------

// deleteNode 节点被删除：其上的 Pod 重建。已绑定卷的可用区约束在重建调度时
// 依然生效 —— 云盘只能挂到同可用区节点，Pod 必须回到原可用区。
func (c *Cluster) deleteNode(name string) {
	n := c.nodes[name]
	if n == nil || n.Dead {
		return
	}
	n.Dead = true
	c.logf("node", "节点 %s（%s）被删除，驱逐其上的 Pod", name, n.Zone)
	for _, p := range c.pods {
		if p.NodeName != name {
			continue
		}
		switch p.Phase {
		case "VolumesBinding":
			c.abortAssume(p, "节点被删除")
		case "Booting", "Running":
			p.Phase = "Pending"
			p.NodeName = ""
			p.Zone = ""
			p.Generation++
			c.logf("node", "Pod %s 随节点删除重建（第 %d 代），已绑定卷约束其只能调度到原可用区", p.Name, p.Generation)
		}
	}
	c.schedulePending()
}

// ---------------- 孤立盘检查 ----------------

// checkOrphans 校验云盘与持久卷的双向引用一致性：
// 每块云盘必须属于某个持久卷，每个动态持久卷必须对应一块真实云盘。
func (c *Cluster) checkOrphans() []string {
	var orphans []string
	for id, d := range c.cloud.disks {
		pv, ok := c.pvs[d.PVName]
		if !ok || pv.DiskID != id {
			orphans = append(orphans, fmt.Sprintf("云盘 %s（%s/%dGi）没有对应的持久卷", id, d.Zone, d.SizeGi))
		}
	}
	for _, pv := range c.pvs {
		if pv.DiskID == "" {
			continue
		}
		if _, ok := c.cloud.disks[pv.DiskID]; !ok {
			orphans = append(orphans, fmt.Sprintf("持久卷 %s 引用的云盘 %s 不存在", pv.Name, pv.DiskID))
		}
	}
	sort.Strings(orphans)
	return orphans
}

// ---------------- 卡住原因诊断 ----------------

func (c *Cluster) diagnose(p *Pod) string {
	for _, pn := range p.PVCNames {
		if c.pvcs[pn].Failed {
			return "云盘供给失败/配额不足"
		}
	}
	zones, wait, _ := c.feasibleZonesForPod(p)
	if wait {
		return "等待卷绑定（供给中）"
	}
	if len(zones) == 0 {
		return "卷拓扑约束无解（多卷可用区交集为空或配额不足）"
	}
	hasBound := false
	for _, pn := range p.PVCNames {
		if c.pvcs[pn].Phase == "Bound" || c.pvcs[pn].Phase == "Resizing" {
			hasBound = true
		}
	}
	resOK, antiOK := false, false
	for _, n := range c.nodes {
		if n.Dead || !zones[n.Zone] {
			continue
		}
		if p.Pool != "" && n.Pool != p.Pool {
			continue
		}
		if n.CPUCap-n.CPUUsed < p.CPU || n.MemCap-n.MemUsed < p.Mem {
			continue
		}
		resOK = true
		if !p.AntiAffinityZone || !c.zoneHasSibling(p, n.Zone) {
			antiOK = true
		}
	}
	if resOK && !antiOK {
		return "副本反亲和约束无法满足"
	}
	if !resOK {
		if hasBound {
			return "卷已绑定可用区与资源不匹配（可用区不匹配）"
		}
		return "集群资源不足"
	}
	return "调度退避中"
}
