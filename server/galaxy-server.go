package server

import (
	"dyzs/galaxy/dispatcher"
	"dyzs/galaxy/logger"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"io"
	"net/http"
	"strings"
	"time"
)

var client = &http.Client{
	Transport: &http.Transport{
		MaxIdleConns:        20,
		MaxIdleConnsPerHost: 5,
		MaxConnsPerHost:     5,
		IdleConnTimeout:     30 * time.Second,
	},
	Timeout: 30 * time.Second,
}

func InitCofnigHttpServer() {

	engin := gin.Default()
	//engin.LoadHTMLGlob("templates/*")

	//初始化配置
	//RouteBoxInit(engin)
	engin.Handle(http.MethodGet, "/debug", func(ctx *gin.Context) {
		level := ctx.Query("level")
		if level != "" {
			logger.ChangeLevel(level)
		}
	})
	engin.Any("/cmd", proxyCmd)

	server := &http.Server{
		Handler: engin,
		Addr:    ":" + viper.GetString("port"),
	}

	err := server.ListenAndServe()
	if err != nil {
		panic(err)
	}
}

func proxyCmd(ctx *gin.Context) {
	address := getAddress(ctx.GetHeader("Target"))
	if address == "" {
		logger.LOG_WARN("未找到目标进程，Target:", ctx.GetHeader("Target"))
		ctx.String(http.StatusServiceUnavailable, "")
		return
	}
	forward("http://"+address+"/cmd", ctx)
}

func forward(url string, ctx *gin.Context) {
	req, err := http.NewRequest(ctx.Request.Method, url, ctx.Request.Body)
	if err != nil {
		fmt.Print("http.NewRequest ", url, ", error:", err.Error())
		return
	}
	for k, v := range ctx.Request.Header {
		req.Header.Set(k, v[0])
	}
	res, err := client.Do(req)
	if err != nil {
		fmt.Print("cli.Do(req) ", url, ", error:", err.Error())
		return
	}
	defer res.Body.Close()
	for k, v := range res.Header {
		ctx.Header(k, v[0])
	}
	_, err = io.Copy(ctx.Writer, res.Body)
	if err != nil {
		fmt.Print("request forward response ", url, ", error:", err.Error())
		return
	}
}

func getAddress(target string) string {
	if target == "" {
		return ""
	}
	if strings.ToLower(target) == "media" {
		return "mediatransfer:7555"
	}
	return dispatcher.GetTaskAddressByResourceId(target)
}
