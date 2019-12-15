package proxy

import (
	modelbase "dag-model/base"
	"dag-model/constant"
	"dag-stream/context"
	"dag-stream/logger"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"net/http"
)

const (
	URL_PREFIX = "/api/dag/component/v2"
)

func StartManagerProxy() {
	server := gin.Default()
	router := server.Group(URL_PREFIX)
	router.Handle(http.MethodPost, "/Init", Init)
	router.Handle(http.MethodPost, "/KeepAlive", KeepAlive)
	router.Handle(http.MethodPost, "/QueryMaxCapacity", QueryMaxCapacity)
	router.Handle(http.MethodPost, "/AssignResource", AssignResource)
	router.Handle(http.MethodPost, "/UpdateResource", AssignResource)
	router.Handle(http.MethodPost, "/RevokeResource", RevokeResource)
	go server.Run(":7777")
}

type ReponseBody struct {
	Code int         `json:"code"`
	Msg  interface{} `json:"msg"`
}

func response(c *gin.Context, code int, data interface{}) {
	if data == nil {
		data = struct{}{}
	}
	c.JSON(code, ReponseBody{
		Code: code,
		Msg:  data,
	})
}

//初始化
func Init(ctx *gin.Context) {
	params := &modelbase.ComponentInitParam{}
	ctx.BindJSON(params)
	//节点id
	context.Set(context.PARAM_KEY_NODE_ID, params.NodeId)
	//组件id
	context.Set(context.PARAM_KEY_COMPONENT_ID, params.ComponentId)
	//节点ip
	context.Set(context.PARAM_KEY_NODE_ADDR, params.NodeAddr)
	response(ctx, http.StatusOK, nil)
}

//心跳接口
func KeepAlive(ctx *gin.Context) {
	response(ctx, http.StatusOK, nil)
}

//获取组件接入容量
func QueryMaxCapacity(ctx *gin.Context) {
	response(ctx, http.StatusOK, struct {
		Ability int `json:"ability"`
	}{
		Ability: 9999999,
	})
}

//下发资源
func AssignResource(ctx *gin.Context) {
	var err error
	var res json.RawMessage
	ar := &modelbase.AssignResource{
		ResourceList: &res,
	}
	err = ctx.BindJSON(ar)
	if err == nil {
		switch ar.ResourceType {
		case constant.ASSIGN_CONFIG_ACCESS:
			fallthrough
		case constant.ASSIGN_CONFIG_TRANSMIT:
			c := make([]map[string]interface{}, 0)
			err = json.Unmarshal(res, &c)
			if err == nil {
				context.AssignConfig(c)
			}
		case constant.ASSIGN_EQUIPMENT:
			eqs := make([]*modelbase.AssignEquipment, 0)
			err = json.Unmarshal(res, &eqs)
			if err == nil {
				context.AssignEquipments(eqs)
			}
		case constant.ASSIGN_TOLLGATE:
			tgs := make([]*modelbase.AssignTollgate, 0)
			err = json.Unmarshal(res, &tgs)
			if err == nil {
				context.AssignTollgates(tgs)
			}
		}
	}
	if err != nil {
		logger.LOG_WARN("配置参数解析失败", err)
		response(ctx, http.StatusBadRequest, err)
	} else {
		response(ctx, http.StatusOK, nil)
	}
}

//回收资源
func RevokeResource(ctx *gin.Context) {
	var err error
	var res json.RawMessage
	ar := &modelbase.AssignResource{
		ResourceList: &res,
	}
	err = ctx.BindJSON(ar)
	if err == nil {
		switch ar.ResourceType {
		case constant.ASSIGN_EQUIPMENT:
			eqs := make([]*modelbase.AssignEquipment, 0)
			err = json.Unmarshal(res, &eqs)
			if err == nil {
				context.RevokeEquipments(eqs)
			}
		case constant.ASSIGN_TOLLGATE:
			tgs := make([]*modelbase.AssignTollgate, 0)
			err = json.Unmarshal(res, &tgs)
			if err == nil {
				context.RevokeTollgates(tgs)
			}
		}
	}
	if err != nil {
		logger.LOG_WARN("配置参数解析失败", err)
		response(ctx, http.StatusBadRequest, err)
	} else {
		response(ctx, http.StatusOK, nil)
	}
}
