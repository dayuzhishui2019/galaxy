package server

import (
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"net/http"
)

func InitCofnigHttpServer() {

	engin := gin.Default()
	engin.LoadHTMLGlob("templates/*")

	//初始化配置
	RouteBoxInit(engin)

	server := &http.Server{
		Handler: engin,
		Addr:    ":" + viper.GetString("port"),
	}
	err := server.ListenAndServe()
	if err != nil {
		panic(err)
	}
}
