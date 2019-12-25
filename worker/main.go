package main

import (
	"flag"
	"fmt"
	"runtime"
	"time"

	"crontab/worker/worker"
)

func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func initArgs() {
	flag.StringVar(&configFile, "config", "./worker.json", "指定配置文件路径")
}

func init() {
	initEnv()
	initArgs()
}

var (
	configFile string
)

func main() {
	var (
		err error
	)
	// 加载配置
	if err = worker.InitConfig(configFile); err != nil {
		goto ERR
	}
	// 服务注册
	if err = worker.InitRegister(); err != nil {
		goto ERR
	}
	// 启动日志协程
	if err = worker.InitLogSink(); err != nil {
		goto ERR
	}
	// 启动执行器
	if err = worker.InitExecutor(); err != nil {
		goto ERR
	}

	// 启动调度器
	if err = worker.InitScheduler(); err != nil {
		goto ERR
	}

	// 初始化任务管理器
	if err = worker.InitJobMgr(); err != nil {
		goto ERR
	}

	for {
		time.Sleep(1 * time.Second)
	}
	// return
ERR:
	fmt.Println(err)
}
