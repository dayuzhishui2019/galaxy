package server

import (
	"dyzs/galaxy/dispatcher"
	"dyzs/galaxy/logger"
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"io"
	"net/http"
	"strings"
	"time"
)

func (chs *ConfigHttpServer) proxy(ctx *gin.Context) {
	target := ctx.GetHeader("Target")
	address := getAddress(target)
	if address == "" {
		logger.LOG_WARN("未找到目标进程，Target:", ctx.GetHeader("Target"))
		ctx.String(http.StatusServiceUnavailable, "")
		return
	}
	chs.forward("http://"+address+"/cmd", ctx)
}

func getAddress(target string) string {
	if target == "" {
		return ""
	}
	if strings.ToLower(target) == "media" {
		return viper.GetString("mediaAddress")
	}
	return dispatcher.GetTaskAddressByResourceId(target)
}

func (chs *ConfigHttpServer) forward(url string, ctx *gin.Context) {
	start := time.Now()
	defer func() {
		logger.LOG_INFO("galaxy-forward耗时：", time.Since(start), ",url:", url)
	}()
	req, err := http.NewRequest(ctx.Request.Method, url, ctx.Request.Body)
	if err != nil {
		logger.LOG_WARN("http.NewRequest ", url, ", error:", err.Error())
		return
	}
	for k, v := range ctx.Request.Header {
		req.Header.Set(k, v[0])
	}
	res, err := chs.client.Do(req)
	if err != nil {
		logger.LOG_WARN("cli.Do(req) ", url, ", error:", err.Error())
		return
	}
	defer res.Body.Close()
	for k, v := range res.Header {
		ctx.Header(k, v[0])
	}
	_, err = io.Copy(ctx.Writer, res.Body)
	if err != nil {
		logger.LOG_WARN("request forward response ", url, ", error:", err.Error())
		return
	}
}
