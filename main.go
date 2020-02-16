package main

import (
	"dyzs/galaxy/dispatcher"
	"dyzs/galaxy/logger"
	"dyzs/galaxy/server"
	"dyzs/galaxy/util"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/json-iterator/go/extra"
	"github.com/spf13/viper"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"
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

func link(c *websocket.Conn) {
	c.SetCloseHandler(func(code int, text string) error {
		fmt.Println("conn 关闭")
		return nil
	})
	//接收数据
	go func() {
		for {
			time.Sleep(5 * time.Second)
			_ = c.WriteJSON(&WsMessage{
				Content: map[string]interface{}{
					"subscribe":   []string{"a1", "a2"},
					"unSubscribe": []string{"b1", "b2"},
				},
			})
		}
	}()
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
