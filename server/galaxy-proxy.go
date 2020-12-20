package server

import (
	"dyzs/galaxy/dispatcher"
	"dyzs/galaxy/logger"
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

func (chs *ConfigHttpServer) proxy(ctx *gin.Context) {
	//判断是否需要记录来源（异步响应）
	//if ctx.GetHeader("TargetFrom") != "" {
	//	chs.syncSessionMap[ctx.GetHeader("TargetFrom")] = ctx.Request.RemoteAddr
	//	fmt.Println("TargetFrom:", ctx.GetHeader("TargetFrom"))
	//	fmt.Println("TargetFromAddr:", ctx.Request.RemoteAddr)
	//}

	target := strings.ToLower(ctx.GetHeader("Target"))
	logger.LOG_INFO("【Target】", ctx.GetHeader("Target"), "【TargetId】", ctx.GetHeader("TargetId"))
	var address string
	if target == "media" {
		//媒体
		address = viper.GetString("mediaAddress")
	} else if target == "from" {
		//targetId,寻找异步消息目的地
		address = ctx.GetHeader("TargetId")
		//fmt.Println("TargetId:", ctx.GetHeader("TargetId"))
		//fmt.Println("TargetId-address:", address)
		//delete(chs.syncSessionMap, ctx.GetHeader("TargetId"))
	} else {
		//设备id：寻找组件
		address = dispatcher.GetTaskAddressByResourceId(target)
	}
	if address == "" {
		logger.LOG_WARN("未找到目标进程，Target:", ctx.GetHeader("Target"), ",调用来源：", ctx.Request.RemoteAddr)
		ctx.String(http.StatusServiceUnavailable, "")
		return
	}
	if strings.HasPrefix(address, "center:") {
		defer func() {
			ctx.JSON(http.StatusOK, &GalaxyResponse{
				Code:    http.StatusOK,
				Message: "success",
			})
		}()
		token := strings.TrimPrefix(address, "center:")
		ts := strings.Split(token, ",")
		if len(ts) == 4 {
			reqBytes, err := ioutil.ReadAll(ctx.Request.Body)
			if err != nil {
				logger.LOG_WARN("读取异步请求异常：", err)
				return
			}
			err = chs.previewWs.asyncResponse(ts[1], ts[2], ts[3], reqBytes)
			if err != nil {
				logger.LOG_WARN("异步响应中心异常：", err)
			}
		} else {
			logger.LOG_WARN("未识别的TargetFrom:" + address)
		}
	} else {
		chs.forward("http://"+address+"/cmd", ctx)
	}
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
	logger.LOG_INFO("galaxy-forward：", "url:", url)
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
