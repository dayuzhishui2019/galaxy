package server

import (
	"bytes"
	"dyzs/galaxy/logger"
	"dyzs/galaxy/util"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/viper"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

const _URL_CENTER_SUBMIT_CHANNEL = "http://{CENTER_IP}:{CENTER_PORT}/management/sensor/submitChannels"

func (chs *ConfigHttpServer) galaxyHandle(c *gin.Context) {
	buff := bytes.NewBuffer(make([]byte, 0, c.Request.ContentLength))
	_, err := buff.ReadFrom(c.Request.Body)
	if err != nil {
		logger.LOG_ERROR("读取数据流失败", err)
		c.JSON(http.StatusBadRequest, map[string]interface{}{
			"code":    http.StatusBadRequest,
			"message": err,
		})
		return
	}
	cmd := &GalaxyCmd{}
	err = jsoniter.Unmarshal(buff.Bytes(), cmd)
	if err != nil {
		logger.LOG_ERROR("json解析失败", err, ",", buff.String())
		c.JSON(http.StatusBadRequest, map[string]interface{}{
			"code":    http.StatusBadRequest,
			"message": err,
		})
		return
	}

	switch cmd.Cmd {
	case "ResponseCatalog":
		chs.submitChannel(c, cmd.Param)
	case "QueryChannelList":
		chs.getAllChannels(c, cmd.Param)
	case "QueryDeviceId":
		chs.getResourceByChannel(c, cmd.Param)
	default:
		logger.LOG_WARN("未找到指令匹配的处理器：", cmd.Cmd)
	}
}

/**
提交设备通道
*/
func (chs *ConfigHttpServer) submitChannel(c *gin.Context, param jsoniter.RawMessage) {
	catalogListParam := &CatalogListParam{}
	fmt.Println(string(param))
	err := jsoniter.Unmarshal(param, catalogListParam)
	if err != nil {
		logger.LOG_WARN("参数解析异常:", err)
		return
	}
	if len(catalogListParam.CatalogList) > 0 {
		channelsReq := make(map[string][]map[string]interface{})
		for _, c := range catalogListParam.CatalogList {
			chs.channelMap[c.DeviceID] = c
			if channelsReq[c.FromID] == nil {
				channelsReq[c.FromID] = make([]map[string]interface{}, 0)
			}
			channelsReq[c.FromID] = append(channelsReq[c.FromID], map[string]interface{}{
				"channelNo": c.DeviceID,
				"id":        "",
				"name":      c.Name,
				"parentId":  "",
			})
		}
		//上报中心
		for deviceId, c := range channelsReq {
			err = util.Retry(func() error {
				return chs.request(strings.ReplaceAll(strings.ReplaceAll(_URL_CENTER_SUBMIT_CHANNEL, "{CENTER_IP}", viper.GetString("center.host")), "{CENTER_PORT}", viper.GetString("center.managePort")), http.MethodPost, "application/json", map[string]interface{}{
					"channels": c,
					"gid":      deviceId,
				}, nil)
			}, 3, 1*time.Second)
			if err != nil {
				logger.LOG_WARN("上报通道信息异常：", err)
				continue
			}
		}
	}
	c.JSON(http.StatusOK, &GalaxyResponse{
		Code:    http.StatusOK,
		Message: "success",
	})
}

/**
查询所有设备通道
*/
func (chs *ConfigHttpServer) getAllChannels(c *gin.Context, param jsoniter.RawMessage) {
	channels := make([]*Channel, 0)
	for _, c := range chs.channelMap {
		channels = append(channels, c)
	}
	c.JSON(http.StatusOK, &GalaxyResponse{
		Code:    http.StatusOK,
		Message: "success",
		Result: map[string]interface{}{
			"ChannelList": channels,
		},
	})
}

/**
通过id查询设备通道
*/
func (chs *ConfigHttpServer) getResourceByChannel(c *gin.Context, param jsoniter.RawMessage) {
	p := make(map[string]string)
	err := jsoniter.Unmarshal(param, &p)
	if err != nil {
		logger.LOG_WARN("参数解析异常:", err)
		goto ERR
	}
	if p["Channel"] == "" {
		logger.LOG_WARN("参数解析异常,Channel为空")
		goto ERR
	}
	if ch, ok := chs.channelMap[p["Channel"]]; ok {
		c.JSON(http.StatusOK, &GalaxyResponse{
			Code:    http.StatusOK,
			Message: "success",
			Result: map[string]string{
				"DeviceId": ch.FromID,
			},
		})
		return
	}

ERR:
	c.JSON(http.StatusOK, &GalaxyResponse{
		Code:    -1,
		Message: "未找到channelNo对应的设备",
	})
}

//http请求
func (chs *ConfigHttpServer) request(url, method, contentType string, body interface{}, resPointer interface{}) error {
	var bodyBytes []byte
	var resBytes []byte
	if body != nil {
		bodyBytes, _ = jsoniter.Marshal(body)
	}
	logger.LOG_INFO("http-request:", url)
	if logger.IsDebug() {
		params, err := jsoniter.Marshal(body)
		if err != nil {
			logger.LOG_WARN("http-request-params-error:", err)
		} else {
			logger.LOG_INFO("http-request-params:", string(params))
		}
	}
	err := util.Retry(func() error {
		req, err := http.NewRequest(method, url, bytes.NewReader(bodyBytes))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", contentType)
		res, err := chs.client.Do(req)
		if err != nil {
			return err
		}
		defer func() {
			err := res.Body.Close()
			if err != nil {
				logger.LOG_WARN("关闭res失败", err)
			}
		}()
		resBytes, err = ioutil.ReadAll(res.Body)
		if err != nil {
			return err
		}
		_ = res.Body.Close()
		if res.StatusCode != http.StatusOK {
			return errors.New(string(resBytes))
		}
		return nil
	}, 3, 3*time.Second)
	if err != nil {
		return err
	}
	logger.LOG_INFO("http-response:", string(resBytes))
	if resPointer != nil {
		return jsoniter.Unmarshal(resBytes, resPointer)
	}
	return nil
}
