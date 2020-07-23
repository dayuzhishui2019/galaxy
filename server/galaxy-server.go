package server

import (
	"dyzs/galaxy/logger"
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"net/http"
	"time"
)

func InitCofnigHttpServer() {
	chs := &ConfigHttpServer{}
	chs.Init()
}

type ConfigHttpServer struct {
	server *http.Server
	client *http.Client

	channelMap map[string]*Channel
}

func (chs *ConfigHttpServer) Init() {
	chs.client = &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        20,
			MaxIdleConnsPerHost: 5,
			MaxConnsPerHost:     5,
			IdleConnTimeout:     30 * time.Second,
		},
		Timeout: 30 * time.Second,
	}
	chs.channelMap = make(map[string]*Channel)

	engin := gin.Default()
	//变更日志级别
	engin.Handle(http.MethodGet, "/debug", chs.debug)
	//处理命令
	engin.Any("/cmd", chs.cmd)

	chs.server = &http.Server{
		Handler: engin,
		Addr:    ":" + viper.GetString("port"),
	}

	err := chs.server.ListenAndServe()
	if err != nil {
		panic(err)
	}
}

func (chs *ConfigHttpServer) debug(ctx *gin.Context){
	level := ctx.Query("level")
	if level != "" {
		logger.ChangeLevel(level)
	}
}

func (chs *ConfigHttpServer) cmd(ctx *gin.Context) {
	target := ctx.GetHeader("Target")
	if target == "galaxy" {
		chs.galaxyHandle(ctx)
	}else{
		chs.proxy(ctx)
	}
}
