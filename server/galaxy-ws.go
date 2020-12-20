package server

import (
	"bytes"
	"context"
	"dyzs/galaxy/constants"
	"dyzs/galaxy/logger"
	"dyzs/galaxy/redis"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/viper"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const _WS_PATH = "/exchange/ws/{namespace}/{sid}"
const _WS_NAMESPACE = "video_preview"
const _WS_SEND_MULTI = "sensorMultiple"
const _WS_CONTENT_TYPE_JSON = "application/json"

type WsReceiveMessage struct {
	RequestId   string          `json:"requestId"`
	DayuId      string          `json:"dayuId"`
	From        string          `json:"from"`
	To          string          `json:"to"`
	SendType    string          `json:"sendType"`
	ContentType string          `json:"contentType"`
	Content     json.RawMessage `json:"content"`
	Timestamp   int64           `json:"timestamp"`
}
type WsSendMessage struct {
	RequestId        string      `json:"requestId"`
	From             string      `json:"from"`
	To               string      `json:"to"`
	SendType         string      `json:"sendType"`
	ContentType      string      `json:"contentType"`
	InteractiveModel string      `json:"interactiveModel"`
	Content          interface{} `json:"content"`
	Timestamp        int64       `json:"timestamp"`
}

type VideoCmd struct {
	Cmd    string              `json:"Cmd"`
	Target string              `json:"Target"`
	Param  jsoniter.RawMessage `json:"Param"`
}

type PreviewWebsocket struct {
	e      *ConfigHttpServer
	ws     *websocket.Conn
	wsLock sync.RWMutex

	redisClient *redis.Cache
	ctx         context.Context

	msgC chan *WsReceiveMessage
}

func (e *ConfigHttpServer) initWs() {
	e.previewWs = &PreviewWebsocket{
		ctx:  context.Background(),
		msgC: make(chan *WsReceiveMessage, 10),
		e:    e,
	}

	go e.previewWs.loopHandle()
	e.previewWs.Run()

}

func (pw *PreviewWebsocket) Run() {
	pw.redisClient = redis.NewRedisCache(0, viper.GetString("redis.addr"), redis.FOREVER)
	go pw.initWebsocket()
}

func (pw *PreviewWebsocket) initWebsocket() {
LOOP:
	for {
		time.Sleep(5 * time.Second)
		select {
		case <-pw.ctx.Done():
			break LOOP
		default:
		}
		var ws *websocket.Conn
		pw.wsLock.RLock()
		ws = pw.ws
		pw.wsLock.RUnlock()
		if ws != nil {
			continue
		}
		centerAddr := viper.GetString("center.host")
		wsPort := viper.GetString("center.managePort")
		wsPath := _WS_PATH

		logger.LOG_INFO("wsPort:", wsPort)
		logger.LOG_INFO("wsPath:", wsPath)
		var boxId string
		err := pw.redisClient.StringGet(constants.REDIS_KEY_BOXID, &boxId)
		if err != nil {
			logger.LOG_WARN("redis 获取boxId异常")
			continue
		}
		wsPath = strings.ReplaceAll(strings.ReplaceAll(wsPath, "{namespace}", _WS_NAMESPACE), "{sid}", boxId)
		logger.LOG_INFO("wsPath decode:", wsPath)
		u := url.URL{Scheme: "ws", Host: centerAddr + ":" + wsPort, Path: wsPath}
		ws, _, err = websocket.DefaultDialer.Dial(u.String(), nil)
		pw.wsLock.Lock()
		if err != nil {
			logger.LOG_WARN(err)
			pw.ws = nil
			pw.wsLock.Unlock()
			continue
		}
		logger.LOG_INFO("websocket连接成功")
		pw.ws = ws
		pw.wsLock.Unlock()
		//读取数据
	READ_LOOP:
		for {
			select {
			case <-pw.ctx.Done():
				break LOOP
			default:
			}
			wrap := &WsReceiveMessage{}
			fmt.Println("等待ws消息")
			err := ws.ReadJSON(wrap)
			fmt.Println("收到ws消息")
			if err != nil {
				logger.LOG_WARN(err)
				pw.wsLock.Lock()
				pw.ws = nil
				pw.wsLock.Unlock()
				break READ_LOOP
			}
			select {
			case pw.msgC <- wrap:
			default:
				wrapBytes, _ := jsoniter.Marshal(wrap)
				logger.LOG_WARN("消息队列满，信令丢弃：", string(wrapBytes))
			}
		}
	}
}

func (pw *PreviewWebsocket) loopHandle() {
	for msg := range pw.msgC {
		logger.LOG_INFO("WS_RECEIVE_CONTENT：", string(msg.Content))
		cmd := &VideoCmd{}
		err := jsoniter.Unmarshal(msg.Content, cmd)
		if err != nil {
			logger.LOG_WARN(err)
			continue
		}

		address := getAddress(cmd.Target)
		if address == "" {
			logger.LOG_WARN("未找到目标进程，Target:", cmd.Target)
			continue
		}
		reqUrl := "http://" + address + "/cmd"
		req, err := http.NewRequest(http.MethodPost, reqUrl, bytes.NewReader(msg.Content))
		if err != nil {
			logger.LOG_WARN("http.NewRequest ", reqUrl, ", error:", err.Error())
			continue
		}
		req.Header.Set("TargetFrom", "center:"+msg.DayuId+","+msg.RequestId+","+msg.From+","+msg.To)
		res, err := pw.e.client.Do(req)
		if err != nil {
			logger.LOG_WARN("cli.Do(req) ", reqUrl, ", error:", err.Error())
			continue
		}
		defer func() {
			err := res.Body.Close()
			if err != nil {
				logger.LOG_WARN("关闭res失败", err)
			}
		}()
		resBytes, err := ioutil.ReadAll(res.Body)
		if err != nil {
			logger.LOG_WARN("读取响应异常：", err)
			continue
		}
		//响应
		err = pw.asyncResponse(msg.RequestId, msg.From, msg.To, resBytes)
		if err != nil {
			logger.LOG_WARN("json响应解析异常:", reqUrl, err)
		}
	}
}

func (pw *PreviewWebsocket) asyncResponse(requestId, to, from string, resBytes []byte) error {
	resMsg := make(map[string]interface{})
	err := jsoniter.Unmarshal(resBytes, &resMsg)
	if err != nil {
		logger.LOG_WARN("json响应解析异常:", err)
		logger.LOG_WARN("json响应异常原消息:", string(resBytes))
		return err
	}
	if pw.ws == nil {
		return errors.New("websocket已断开")
	}
	err = pw.ws.WriteJSON(&WsSendMessage{
		RequestId:        requestId,
		InteractiveModel: "ack",
		To:               to,
		From:             from,
		Timestamp:        time.Now().UnixNano() / 1e6,
		ContentType:      _WS_CONTENT_TYPE_JSON,
		SendType:         _WS_SEND_MULTI,
		Content:          resMsg,
	})
	if err != nil {
		logger.LOG_WARN("websocket发送异常：", err)
		return err
	} else {
		logger.LOG_INFO("WS_SEND_CONTENT：", string(resBytes))
	}
	return nil
}
