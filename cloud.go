package main

import (
	"errors"
	"fmt"
	"math/rand"
)

// 云盘供给接口的仿真：按可用区配额限制、供给有耗时、偶尔失败、偶发慢供给。
// 配额在发起供给时即预留（含在途供给），删除盘时释放。

var (
	ErrQuota     = errors.New("可用区云盘配额不足")
	ErrProvision = errors.New("云盘供给失败（后端临时错误）")
)

type Cloud struct {
	sim      *Sim
	rnd      *rand.Rand
	quota    map[string]int // 可用区 -> 配额 Gi
	used     map[string]int // 可用区 -> 已用（含预留）Gi
	disks    map[string]*Disk
	failRate float64
	idSeq    int
}

func (c *Cloud) quotaFree(zone string, size int) bool {
	return c.used[zone]+size <= c.quota[zone]
}

// Provision 异步供给云盘，完成后回调 cb(disk, err)
func (c *Cloud) Provision(zone string, size int, cb func(*Disk, error)) {
	if !c.quotaFree(zone, size) {
		c.sim.After(0.4+c.rnd.Float64()*0.4, func() { cb(nil, ErrQuota) })
		return
	}
	c.used[zone] += size // 预留配额
	c.idSeq++
	d := &Disk{ID: fmt.Sprintf("disk-%04d", c.idSeq), Zone: zone, SizeGi: size, State: "creating"}
	c.disks[d.ID] = d
	latency := 2 + c.rnd.Float64()*6
	if c.rnd.Float64() < 0.08 { // 偶发慢供给
		latency = 16 + c.rnd.Float64()*20
	}
	c.sim.After(latency, func() {
		if c.rnd.Float64() < c.failRate {
			delete(c.disks, d.ID)
			c.used[zone] -= size
			cb(nil, ErrProvision)
			return
		}
		d.State = "available"
		cb(d, nil)
	})
}

// Delete 删除云盘并释放配额
func (c *Cloud) Delete(id string) {
	d, ok := c.disks[id]
	if !ok {
		return
	}
	c.used[d.Zone] -= d.SizeGi
	delete(c.disks, id)
}

// Resize 异步扩容云盘（只支持扩大）
func (c *Cloud) Resize(id string, newSize int, cb func(error)) {
	d, ok := c.disks[id]
	if !ok {
		cb(fmt.Errorf("云盘 %s 不存在", id))
		return
	}
	delta := newSize - d.SizeGi
	if delta > 0 && !c.quotaFree(d.Zone, delta) {
		c.sim.After(0.5, func() { cb(ErrQuota) })
		return
	}
	c.used[d.Zone] += delta
	c.sim.After(1+c.rnd.Float64()*3, func() {
		if c.rnd.Float64() < c.failRate/2 {
			c.used[d.Zone] -= delta
			cb(ErrProvision)
			return
		}
		d.SizeGi = newSize
		cb(nil)
	})
}
