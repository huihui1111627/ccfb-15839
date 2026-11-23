package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP 控制台监听地址")
	seed := flag.Int64("seed", 5, "随机种子")
	serve := flag.Bool("serve", true, "是否启动 HTTP 控制台（false 则只打印对比结果）")
	flag.Parse()

	exp := RunExperiment(*seed)
	printComparison(exp)

	if !*serve {
		return
	}
	if exp.Immediate.OrphanDisks != 0 || exp.Delayed.OrphanDisks != 0 {
		fmt.Fprintln(os.Stderr, "警告：存在孤立盘！")
	}
	startServer(*addr, exp)
}
