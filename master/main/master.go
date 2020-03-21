package main

import (
	"flag"
	"fmt"
	"github.com/zhuhj083/crontab/master"
	"runtime"
	"time"
)

var (
	// 配置文件路径
	confFile string
)

// 解析命令行参数
func initArgs() {
	// master -config ./master.json
	// master -h
	flag.StringVar(&confFile, "config", "./master.json", "指定master.json")
	flag.Parse()
}

func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {

	var (
		err error
	)

	// 初始化命令行参数
	initArgs()

	// 初始化线程
	initEnv()

	// 初始化配置
	if err = master.InitConfig(confFile); err != nil {
		goto ERR
	}

	// 任务管理器
	if err = master.InitJobMgr(); err != nil {
		goto ERR
	}

	// 启动Api Http服务
	if err = master.InitApiServer(); err != nil {
		goto ERR
	}

	// 正常退出
	for {
		time.Sleep(1 * time.Second)
	}

	return

ERR:
	fmt.Println(err)
}