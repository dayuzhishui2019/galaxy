package main

import (
	"dyzs/galaxy/util"
	"github.com/json-iterator/go/extra"
	"github.com/spf13/viper"
	"log"
	"os"
	"os/signal"
	"dyzs/galaxy/dispatcher"
	"dyzs/galaxy/logger"
)

func init() {
	configPath := util.GetAppPath() + "config.yml"
	logger.LOG_INFO("configPath:", configPath)
	viper.SetConfigFile(configPath)
	viper.SetConfigType("yaml")
	if err := viper.ReadInConfig(); err != nil {
		log.Fatal("Fail to read config file :", err)
	}
}

func main() {

	extra.RegisterFuzzyDecoders()

	logger.Init()

	//初始化调度服务
	td := &dispatcher.TaskDispatcher{
		Host: viper.GetString("host"),
	}
	go td.Init()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	<-c
}
