package main

import "fmt"

// 调度器：挑选节点时把卷的拓扑约束（已绑定卷的可用区、延迟绑定卷的可满足
// 可用区）与节点的资源、节点池亲和、副本反亲和一起考虑；选定节点后对延迟
// 绑定的卷做"假定绑定"，假定失败可撤销。

// schedulePending 扫描等待队列，尝试调度每个 Pending Pod
func (c *Cluster) schedulePending() {
	for _, name := range c.podOrder {
		p := c.pods[name]
		if p.Phase != "Pending" {
			continue
		}
		c.trySchedulePod(p)
	}
}

func (c *Cluster) trySchedulePod(p *Pod) {
	// 1. 计算全部卷拓扑约束的交集
	zones, wait, reason := c.feasibleZonesForPod(p)
	if wait {
		return // 立即绑定的卷尚未就绪，静默等待绑定完成事件
	}
	if len(zones) == 0 {
		p.LastReason = reason
		c.logf("schedule", "Pod %s 无可行可用区：%s", p.Name, reason)
		return
	}
	// 2. 在可行可用区内过滤节点：资源、节点池亲和、副本反亲和；按可用区保留最优节点
	byZone := map[string]*Node{}
	for _, name := range c.nodeOrder {
		n := c.nodes[name]
		if n.Dead || !zones[n.Zone] {
			continue
		}
		if n.CPUCap-n.CPUUsed < p.CPU || n.MemCap-n.MemUsed < p.Mem {
			continue
		}
		if p.Pool != "" && n.Pool != p.Pool {
			continue
		}
		if p.AntiAffinityZone && c.zoneHasSibling(p, n.Zone) {
			continue
		}
		if cur := byZone[n.Zone]; cur == nil || n.CPUCap-n.CPUUsed > cur.CPUCap-cur.CPUUsed {
			byZone[n.Zone] = n
		}
	}
	if len(byZone) == 0 {
		p.LastReason = "可行可用区内无满足资源/节点池/反亲和的节点"
		c.logf("schedule", "Pod %s 暂不可调度：卷约束可行域 %v，但无满足资源（cpu=%d,mem=%dGi）/节点池（%q）/反亲和的节点",
			p.Name, sortedKeys(zones), p.CPU, p.Mem, p.Pool)
		return
	}
	// 3. 选择可用区与节点：需要动态供给时优先云盘配额宽裕的可用区，
	//    让各可用区配额使用保持均衡；否则选空闲处理器最多的节点。
	needDynamic := false
	for _, pn := range p.PVCNames {
		pvc := c.pvcs[pn]
		if pvc.Phase == "Pending" && c.scs[pvc.SC].Mode == BindWaitForConsumer {
			needDynamic = true
		}
	}
	var best *Node
	bestFrac, quotaNote := -1.0, ""
	for z, n := range byZone {
		frac := 1.0
		if needDynamic {
			frac = float64(c.cloud.quota[z]-c.cloud.used[z]) / float64(c.cloud.quota[z])
		}
		if frac > bestFrac ||
			(frac == bestFrac && best != nil && n.CPUCap-n.CPUUsed > best.CPUCap-best.CPUUsed) {
			bestFrac, best = frac, n
			quotaNote = ""
			if needDynamic {
				quotaNote = fmt.Sprintf("，配额感知：%s 云盘余量 %.0f%%", z, frac*100)
			}
		}
	}
	c.logf("schedule", "调度 Pod %s → 节点 %s（%s）：卷拓扑约束交集 %v，节点池=%q，反亲和校验通过%s",
		p.Name, best.Name, best.Zone, sortedKeys(zones), p.Pool, quotaNote)
	// 4. 对延迟绑定的卷做假定绑定
	c.assumeVolumes(p, best)
}

// feasibleZonesForPod 计算 Pod 所有卷拓扑约束的交集。
// 返回 (可行可用区集合, 是否需等待卷就绪, 不可行原因)
func (c *Cluster) feasibleZonesForPod(p *Pod) (map[string]bool, bool, string) {
	acc := map[string]bool{}
	for _, z := range c.zoneList {
		acc[z] = true
	}
	for _, pn := range p.PVCNames {
		pvc := c.pvcs[pn]
		sc := c.scs[pvc.SC]
		switch {
		case pvc.Phase == "Bound" || pvc.Phase == "Resizing" || pvc.Phase == "Assumed":
			// 已绑定卷：云盘只能挂到同可用区节点
			acc = intersectZones(acc, map[string]bool{pvc.Zone: true})
			if len(acc) == 0 {
				return acc, false, "多个已绑定卷位于不同可用区（" + pvc.Name + " 在 " + pvc.Zone + "），拓扑约束交集为空"
			}
		case pvc.Failed:
			return nil, false, "声明 " + pvc.Name + " 供给失败：" + pvc.FailedReason
		case sc.Mode == BindImmediate:
			// 立即绑定的卷还没绑好，Pod 只能等
			return nil, true, ""
		default:
			// 延迟绑定的卷：约束是"能满足该卷的可用区集合"
			sat := map[string]bool{}
			for _, z := range c.zoneList {
				if c.pvcSatisfiableInZone(pvc, z) {
					sat[z] = true
				}
			}
			acc = intersectZones(acc, sat)
			if len(acc) == 0 {
				return acc, false, "卷 " + pvc.Name + " 加入后拓扑约束交集为空（各可用区配额/空闲卷均不满足）"
			}
		}
	}
	return acc, false, ""
}

// pvcSatisfiableInZone 延迟绑定卷在某可用区是否可满足：有匹配的静态空闲卷，
// 或该可用区配额足够动态供给。
func (c *Cluster) pvcSatisfiableInZone(pvc *PVC, zone string) bool {
	if c.findAvailablePV(pvc, zone) != nil {
		return true
	}
	sc := c.scs[pvc.SC]
	return sc.DynamicProvision && c.cloud.quotaFree(zone, pvc.RequestGi)
}

// zoneHasSibling 同服务的其他副本是否已占用了该可用区（反亲和检查）
func (c *Cluster) zoneHasSibling(p *Pod, zone string) bool {
	for _, q := range c.pods {
		if q.Service == p.Service && q.Name != p.Name && q.Zone == zone && q.Phase != "Pending" {
			return true
		}
	}
	return false
}

// assumeVolumes 选定节点后，对延迟绑定的卷做假定绑定：
// 有静态空闲卷则预占，否则在该节点可用区触发动态供给。
// 假定设有超时；供给失败或超时都会撤销假定（见 abortAssume）。
func (c *Cluster) assumeVolumes(p *Pod, node *Node) {
	p.NodeName = node.Name
	p.Zone = node.Zone
	p.Phase = "VolumesBinding"
	node.CPUUsed += p.CPU
	node.MemUsed += p.Mem
	p.AssumeGen++
	gen := p.AssumeGen
	for _, pn := range p.PVCNames {
		pvc := c.pvcs[pn]
		if pvc.Phase == "Bound" || pvc.Phase == "Resizing" {
			continue // 已绑定卷（如立即绑定模式下已就绪）
		}
		sc := c.scs[pvc.SC]
		if sc.Mode == BindImmediate {
			continue
		}
		pvc.Phase = "Assumed"
		pvc.AssumedBy = p.Name
		pvc.Zone = node.Zone
		if pv := c.findAvailablePV(pvc, node.Zone); pv != nil {
			pv.Phase = "Reserved"
			pv.ClaimRef = pvc.Name
			pvc.ReservedPV = pv.Name
			c.logf("bind", "假定绑定：声明 %s 预占静态卷 %s（%s）", pvc.Name, pv.Name, pv.Zone)
		} else {
			c.logf("provision", "假定绑定：为声明 %s 在 %s 动态供给 %dGi 云盘", pvc.Name, node.Zone, pvc.RequestGi)
			c.provisionForPVC(pvc, node.Zone)
		}
	}
	c.sim.After(assumeTimeout, func() {
		if p.Phase == "VolumesBinding" && p.AssumeGen == gen {
			c.abortAssume(p, "卷绑定假定超时")
		}
	})
	c.maybeFinishBind(p)
}
