package main

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
)

// ---------------- 集群拓扑 ----------------

// buildClusterTopo 三可用区集群：各可用区节点的处理器/内存规模不同，
// 云盘配额不同；预建一批静态卷（分布在不同可用区、不同存储类）。
func buildClusterTopo(c *Cluster) {
	c.cloud.quota = map[string]int{"zone-a": 650, "zone-b": 550, "zone-c": 350}

	addNode := func(name, zone, pool string, cpu, mem int) {
		c.nodes[name] = &Node{Name: name, Zone: zone, Pool: pool, CPUCap: cpu, MemCap: mem}
		c.nodeOrder = append(c.nodeOrder, name)
	}
	// zone-a 最强，zone-c 最弱 —— 制造资源压力差
	addNode("node-a-1", "zone-a", "compute", 16, 64)
	addNode("node-a-2", "zone-a", "compute", 16, 64)
	addNode("node-a-3", "zone-a", "storage", 12, 48)
	addNode("node-a-4", "zone-a", "storage", 12, 48)
	addNode("node-b-1", "zone-b", "compute", 12, 48)
	addNode("node-b-2", "zone-b", "compute", 12, 48)
	addNode("node-b-3", "zone-b", "storage", 8, 32)
	addNode("node-b-4", "zone-b", "storage", 8, 32)
	addNode("node-c-1", "zone-c", "compute", 8, 32)
	addNode("node-c-2", "zone-c", "compute", 8, 32)
	addNode("node-c-3", "zone-c", "storage", 4, 16)
	addNode("node-c-4", "zone-c", "storage", 4, 16)

	mode := BindImmediate
	if c.mode == "delayed" {
		mode = BindWaitForConsumer
	}
	c.scs["cloud-ssd"] = &StorageClass{Name: "cloud-ssd", Mode: mode, DynamicProvision: true}
	c.scs["cloud-hdd"] = &StorageClass{Name: "cloud-hdd", Mode: mode, DynamicProvision: true}

	// 静态预建卷（消耗对应可用区配额）
	static := []struct {
		sc, zone string
		size     int
	}{
		{"cloud-ssd", "zone-a", 20}, {"cloud-ssd", "zone-a", 30},
		{"cloud-ssd", "zone-b", 20}, {"cloud-ssd", "zone-b", 40},
		{"cloud-ssd", "zone-c", 20},
		{"cloud-hdd", "zone-a", 50},
		{"cloud-hdd", "zone-b", 30},
		{"cloud-hdd", "zone-c", 40},
	}
	for i, s := range static {
		id := fmt.Sprintf("disk-static-%02d", i)
		c.cloud.disks[id] = &Disk{ID: id, Zone: s.zone, SizeGi: s.size, State: "available"}
		c.cloud.used[s.zone] += s.size
		pv := &PV{
			Name: fmt.Sprintf("pv-static-%02d", i), SC: s.sc, Zone: s.zone,
			CapacityGi: s.size, AccessMode: "RWO", Phase: "Available", DiskID: id, Static: true,
		}
		c.cloud.disks[id].PVName = pv.Name
		c.pvs[pv.Name] = pv
	}
}

// ---------------- 工作负载 ----------------

type pvcSpec struct {
	name, sc string
	size     int
}

// scheduleWorkload 生成一批有状态服务：多副本服务要求可用区反亲和，
// 部分 Pod 有节点池亲和要求，部分 Pod 使用多个卷（拓扑约束取交集）。
// 服务在 0~60s 内陆续创建，模拟真实集群的陆续上线。
func scheduleWorkload(c *Cluster, wrnd *rand.Rand) {
	t := 0.0
	for i := 0; i < 33; i++ {
		svcName := fmt.Sprintf("svc-%02d", i)
		replicas := 1
		if r := wrnd.Float64(); r > 0.85 {
			replicas = 3
		} else if r > 0.55 {
			replicas = 2
		}
		c.services[svcName] = &Service{Name: svcName, Replicas: replicas, AntiAffinityZone: replicas > 1}
		c.svcOrder = append(c.svcOrder, svcName)
		for j := 0; j < replicas; j++ {
			pod := &Pod{
				Name:             fmt.Sprintf("%s-%d", svcName, j),
				Service:          svcName,
				CPU:              1 + wrnd.Intn(3),
				Mem:              2 + wrnd.Intn(6),
				AntiAffinityZone: replicas > 1,
				StartedAt:        -1,
			}
			if pr := wrnd.Float64(); pr < 0.65 {
				pod.Pool = "compute"
			} else if pr < 0.9 {
				pod.Pool = "storage"
			}
			nv := 1
			if wrnd.Float64() < 0.3 {
				nv = 2
			}
			var specs []pvcSpec
			for k := 0; k < nv; k++ {
				size := 10 + wrnd.Intn(14) // 10~23Gi
				sc := "cloud-ssd"
				if wrnd.Float64() < 0.4 {
					sc = "cloud-hdd"
				}
				name := fmt.Sprintf("pvc-%s-%d", pod.Name, k)
				specs = append(specs, pvcSpec{name: name, sc: sc, size: size})
				pod.PVCNames = append(pod.PVCNames, name)
			}
			p, sp := pod, specs
			c.sim.After(t, func() {
				for _, s := range sp {
					c.createPVC(s.name, s.sc, s.size, p.Name)
				}
				c.addPod(p)
				c.schedulePending()
			})
		}
		t += 1.5
	}
}

// triggerResizes 随机挑若干已绑定卷做在线扩容
func (c *Cluster) triggerResizes(wrnd *rand.Rand) {
	count := 0
	for _, name := range c.pvcOrder {
		if count >= 8 {
			break
		}
		pvc := c.pvcs[name]
		if pvc.Phase != "Bound" {
			continue
		}
		if wrnd.Float64() < 0.25 {
			c.resizePVC(pvc, pvc.RequestGi+10+wrnd.Intn(11))
			count++
		}
	}
	c.logf("resize", "批量触发 %d 个卷的在线扩容", count)
}

// ---------------- 实验 ----------------

func RunExperiment(seed int64) *Experiment {
	return &Experiment{
		Seed:      seed,
		Immediate: runScenario("immediate", seed),
		Delayed:   runScenario("delayed", seed),
	}
}

func runScenario(mode string, seed int64) *ScenarioReport {
	c := NewCluster(mode, seed)
	wrnd := rand.New(rand.NewSource(seed*7 + 13)) // 两个场景共用同一份负载
	buildClusterTopo(c)
	scheduleWorkload(c, wrnd)
	c.sim.After(100, func() { c.deleteNode("node-b-2") })
	c.sim.After(160, func() { c.triggerResizes(wrnd) })
	c.sim.After(400, func() {
		if orphans := c.checkOrphans(); len(orphans) == 0 {
			c.logf("orphan", "孤立盘检查：通过（0 个孤立盘）")
		} else {
			c.logf("orphan", "孤立盘检查：发现 %d 个孤立盘！%v", len(orphans), orphans)
		}
	})
	c.sim.Run(5000)
	return c.buildReport()
}

// ---------------- 报告 ----------------

func (c *Cluster) buildReport() *ScenarioReport {
	r := &ScenarioReport{
		Mode:         c.mode,
		SimDuration:  math.Round(c.sim.Now*10) / 10,
		StuckReasons: map[string]int{},
		TotalPods:    len(c.podOrder),
	}
	var ttrs, binds []float64
	for _, name := range c.podOrder {
		p := c.pods[name]
		ps := PodSnap{
			Name: p.Name, Service: p.Service, Phase: p.Phase, Node: p.NodeName,
			Zone: p.Zone, PVCs: p.PVCNames, CPU: p.CPU, Mem: p.Mem,
			Reschedules: p.Reschedules, TTR: -1,
		}
		if p.Phase == "Running" {
			r.RunningPods++
			ttr := p.StartedAt - p.CreatedAt
			ttrs = append(ttrs, ttr)
			ps.TTR = math.Round(ttr*10) / 10
		} else {
			r.StuckPods++
			reason := c.diagnose(p)
			ps.Reason = reason
			r.StuckReasons[reason]++
		}
		r.Pods = append(r.Pods, ps)
	}
	r.TTRAvg, r.TTRP50, r.TTRP95, r.TTRMax = stats(ttrs)

	for _, name := range c.pvcOrder {
		pvc := c.pvcs[name]
		snap := PVCSnap{
			Name: pvc.Name, SC: pvc.SC, Mode: string(c.scs[pvc.SC].Mode),
			Phase: pvc.Phase, Zone: pvc.Zone, Volume: pvc.VolumeName,
			Pod: pvc.PodName, ReqGi: pvc.RequestGi, BoundWait: -1,
			Failed: pvc.Failed, Reason: pvc.FailedReason,
		}
		if pvc.BoundAt >= 0 {
			w := pvc.BoundAt - pvc.CreatedAt
			binds = append(binds, w)
			snap.BoundWait = math.Round(w*10) / 10
		}
		r.PVCs = append(r.PVCs, snap)
	}
	r.BindAvg, _, r.BindP95, _ = stats(binds)

	// 反亲和满足率：多副本服务全部副本运行且分布在不同可用区
	for _, svcName := range c.svcOrder {
		svc := c.services[svcName]
		if !svc.AntiAffinityZone {
			continue
		}
		r.AntiTotal++
		zones := map[string]bool{}
		ok := true
		for _, pn := range c.podOrder {
			p := c.pods[pn]
			if p.Service != svcName {
				continue
			}
			if p.Phase != "Running" {
				ok = false
				break
			}
			zones[p.Zone] = true
		}
		if ok && len(zones) == svc.Replicas {
			r.AntiSatisfied++
		}
	}

	r.OrphanList = c.checkOrphans()
	r.OrphanDisks = len(r.OrphanList)

	var pcts []float64
	for _, z := range c.zoneList {
		q, u := c.cloud.quota[z], c.cloud.used[z]
		pct := 0.0
		if q > 0 {
			pct = float64(u) / float64(q)
		}
		pcts = append(pcts, pct)
		r.Zones = append(r.Zones, ZoneSnap{Name: z, Quota: q, Used: u, Pct: math.Round(pct*1000) / 10})
	}
	r.QuotaBalance = math.Round(stddev(pcts)*1000) / 1000

	for _, name := range c.nodeOrder {
		n := c.nodes[name]
		r.Nodes = append(r.Nodes, NodeSnap{
			Name: n.Name, Zone: n.Zone, Pool: n.Pool,
			CPUCap: n.CPUCap, CPUUsed: n.CPUUsed,
			MemCap: n.MemCap, MemUsed: n.MemUsed, Dead: n.Dead,
		})
	}

	r.ProvisionFails = c.provisionFails
	r.AssumeAborts = c.assumeAborts
	r.OrphanReclaims = c.orphanReclaims
	r.Reschedules = c.reschedules

	r.Events = c.logs
	if len(r.Events) > 1500 {
		r.Events = r.Events[len(r.Events)-1500:]
	}
	return r
}

func stats(vals []float64) (avg, p50, p95, max float64) {
	if len(vals) == 0 {
		return 0, 0, 0, 0
	}
	sort.Float64s(vals)
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	avg = sum / float64(len(vals))
	p50 = vals[len(vals)/2]
	p95 = vals[int(math.Ceil(0.95*float64(len(vals))))-1]
	max = vals[len(vals)-1]
	round := func(x float64) float64 { return math.Round(x*10) / 10 }
	return round(avg), round(p50), round(p95), round(max)
}

func stddev(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	mean := sum / float64(len(vals))
	sq := 0.0
	for _, v := range vals {
		sq += (v - mean) * (v - mean)
	}
	return math.Sqrt(sq / float64(len(vals)))
}

// printComparison 终端打印两种绑定模式的对比
func printComparison(e *Experiment) {
	a, b := e.Immediate, e.Delayed
	row := func(label string, va, vb string) {
		fmt.Printf("  %-38s %-18s %s\n", label, va, vb)
	}
	pct := func(x, y int) string {
		if y == 0 {
			return "-"
		}
		return fmt.Sprintf("%.0f%%", float64(x)/float64(y)*100)
	}
	zonePct := func(r *ScenarioReport) string {
		s := ""
		for i, z := range r.Zones {
			if i > 0 {
				s += " / "
			}
			s += fmt.Sprintf("%.0f%%", z.Pct)
		}
		return s
	}
	fmt.Printf("\n================== 仿真实验对比（种子 %d，三可用区集群） ==================\n", e.Seed)
	fmt.Printf("  %-38s %-18s %s\n", "指标", "全部立即绑定", "延迟绑定")
	fmt.Println("  " + string(make([]byte, 72)))
	row("运行中的容器", fmt.Sprintf("%d/%d", a.RunningPods, a.TotalPods), fmt.Sprintf("%d/%d", b.RunningPods, b.TotalPods))
	row("因约束卡住的容器数", fmt.Sprintf("%d", a.StuckPods), fmt.Sprintf("%d", b.StuckPods))
	reasons := map[string]bool{}
	for k := range a.StuckReasons {
		reasons[k] = true
	}
	for k := range b.StuckReasons {
		reasons[k] = true
	}
	var rk []string
	for k := range reasons {
		rk = append(rk, k)
	}
	sort.Strings(rk)
	for _, k := range rk {
		row("    · "+k, fmt.Sprintf("%d", a.StuckReasons[k]), fmt.Sprintf("%d", b.StuckReasons[k]))
	}
	row("副本跨可用区反亲和满足率", pct(a.AntiSatisfied, a.AntiTotal), pct(b.AntiSatisfied, b.AntiTotal))
	row("创建到运行平均时长", fmt.Sprintf("%.1fs", a.TTRAvg), fmt.Sprintf("%.1fs", b.TTRAvg))
	row("创建到运行 P95 时长", fmt.Sprintf("%.1fs", a.TTRP95), fmt.Sprintf("%.1fs", b.TTRP95))
	row("卷平均绑定时长", fmt.Sprintf("%.1fs", a.BindAvg), fmt.Sprintf("%.1fs", b.BindAvg))
	row("供给失败次数（含重试）", fmt.Sprintf("%d", a.ProvisionFails), fmt.Sprintf("%d", b.ProvisionFails))
	row("调度假定撤销次数", fmt.Sprintf("%d", a.AssumeAborts), fmt.Sprintf("%d", b.AssumeAborts))
	row("撤销后回收的云盘数", fmt.Sprintf("%d", a.OrphanReclaims), fmt.Sprintf("%d", b.OrphanReclaims))
	row("孤立盘数量（必须为 0）", fmt.Sprintf("%d", a.OrphanDisks), fmt.Sprintf("%d", b.OrphanDisks))
	row("各可用区云盘配额使用率(a/b/c)", zonePct(a), zonePct(b))
	row("配额使用均衡度(标准差,越小越好)", fmt.Sprintf("%.3f", a.QuotaBalance), fmt.Sprintf("%.3f", b.QuotaBalance))
	fmt.Println()
}
