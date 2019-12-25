package main

import (
	"flag"
	"fmt"
	"runtime"
	"time"

	"cron-master/master"
)

func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func initArgs() {
	flag.StringVar(&configFile, "config", "./cron-master.json", "指定配置文件路径")
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
	if err = master.InitConfig(configFile); err != nil {
		goto ERR
	}
	// 初始化服务发现模块
	if err = master.InitWorkerMgr(); err != nil {
		goto ERR
	}
	if err = master.InitLogMgr(); err != nil {
		goto ERR
	}
	if err = master.InitJobMgr(); err != nil {
		goto ERR
	}
	// 启动Api HTTP服务器
	if err = master.InitAPIServer(); err != nil {
		goto ERR
	}

	for {
		time.Sleep(1 * time.Second)
	}
	// return
ERR:
	fmt.Println(err)
}
