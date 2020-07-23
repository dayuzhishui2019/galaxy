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
)

func (chs *ConfigHttpServer) proxy(ctx *gin.Context){
	target := ctx.GetHeader("Target")
	address := chs.getAddress(target)
	if address == "" {
		logger.LOG_WARN("未找到目标进程，Target:", ctx.GetHeader("Target"))
		ctx.String(http.StatusServiceUnavailable, "")
		return
	}
	chs.forward("http://"+address+"/cmd", ctx)
}

func (chs *ConfigHttpServer) getAddress(target string) string {
	if target == "" {
		return ""
	}
	if strings.ToLower(target) == "media" {
		return viper.GetString("mediaAddress")
	}
	return dispatcher.GetTaskAddressByResourceId(target)
}

func (chs *ConfigHttpServer) forward(url string, ctx *gin.Context) {
	req, err := http.NewRequest(ctx.Request.Method, url, ctx.Request.Body)
	if err != nil {
		fmt.Print("http.NewRequest ", url, ", error:", err.Error())
		return
	}
	for k, v := range ctx.Request.Header {
		req.Header.Set(k, v[0])
	}
	res, err := chs.client.Do(req)
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
