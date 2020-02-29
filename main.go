package main

import (
	"dyzs/galaxy/dispatcher"
	"dyzs/galaxy/logger"
	"dyzs/galaxy/server"
	"dyzs/galaxy/util"
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/json-iterator/go/extra"
	"github.com/spf13/viper"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
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

	//startWebsocket()

	initSN()

	extra.RegisterFuzzyDecoders()

	logger.Init()

	//初始化配置服务
	go server.InitCofnigHttpServer()

	//初始化调度服务
	td := &dispatcher.TaskDispatcher{
		Host: viper.GetString("host"),
	}
	go td.Init()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	<-c
}

func initSN() {
	sn := viper.GetString("sn")
	if sn != "" {
		logger.LOG_WARN("获取sn：", sn)
		return
	}
	snCmd := viper.GetString("snCmd")
	cmdRes, err := util.ExecCmd(snCmd)
	if err != nil {
		logger.LOG_ERROR("获取sn异常：", err)
		return
	}
	logger.LOG_WARN("获取sn成功：", cmdRes)
	ss := strings.Split(cmdRes, ":")

	if len(ss) == 2 {
		sn = strings.ReplaceAll(strings.Trim(ss[1], " "), " ", "")
		if len(sn) > 48 {
			sn = sn[:48]
		}
	}
	logger.LOG_WARN("获取sn：", sn)
	viper.Set("sn", sn)
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type WsMessage struct {
	From        string      `json:"from"`
	To          string      `json:"to"`
	SendType    string      `json:"sendType"`
	ContentType string      `json:"contentType"`
	Content     interface{} `json:"content"`
	Timestamp   int64       `json:"timestamp"`
}
type WsReceiveMessage struct {
	From        string          `json:"from"`
	To          string          `json:"to"`
	SendType    string          `json:"sendType"`
	ContentType string          `json:"contentType"`
	Content     json.RawMessage `json:"content"`
	Timestamp   int64           `json:"timestamp"`
}
func link(ws *websocket.Conn) {
	ws.SetCloseHandler(func(code int, text string) error {
		fmt.Println("conn 关闭")
		return nil
	})
	//接收数据
	//读取数据
READ_LOOP:
	for {
		wrap := &WsReceiveMessage{}
		err := ws.ReadJSON(wrap)
		if err != nil {
			logger.LOG_WARN(err)
			break READ_LOOP
		}
		str,_ := json.Marshal(wrap)
		fmt.Println(string(str))
fmt.Println(len(str))
	}
}

func startWebsocket() {
	//启动websocket-server
	server := http.NewServeMux()
	server.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("收到")
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Print("upgrade:", err)
			return
		}
		fmt.Println("升级")
		link(c)
	})
	http.ListenAndServe(":7070", server)
}
