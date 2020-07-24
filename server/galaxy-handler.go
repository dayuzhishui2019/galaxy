package server

import (
	"bytes"
	"dyzs/galaxy/logger"
	"github.com/gin-gonic/gin"
	jsoniter "github.com/json-iterator/go"
	"net/http"
)

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
	err := jsoniter.Unmarshal(param, catalogListParam)
	if err != nil {
		logger.LOG_WARN("参数解析异常:", err)
		return
	}
	if len(catalogListParam.CatalogList) > 0 {
		for _, c := range catalogListParam.CatalogList {
			chs.channelMap[c.DeviceID] = c
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
	channels := make([]*Channel, len(chs.channelMap))
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
	if p["channelNo"] == "" {
		logger.LOG_WARN("参数解析异常,channelNo为空")
		goto ERR
	}
	if ch, ok := chs.channelMap[p["channelNo"]]; ok {
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
