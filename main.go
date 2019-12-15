package main

import (
	"github.com/json-iterator/go/extra"
	"github.com/spf13/viper"
	"os"
	"os/signal"
	_ "sunset/data-hub/context"
	"sunset/data-hub/db/mongo"
	"sunset/data-hub/dispatcher"
	"sunset/data-hub/logger"
	"sunset/data-hub/route"
)

func main() {

	extra.RegisterFuzzyDecoders()

	logger.Init()

	//数据库连接
	_ = mongo.Connect()

	//初始化配置服务
	go route.InitCofnigHttpServer()

	//初始化调度服务
	d := &dispatcher.TaskDispatcher{
		Host: viper.GetString("host"),
	}
	go d.Init()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	<-c
}
