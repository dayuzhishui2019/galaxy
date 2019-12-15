package route

import (
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"net/http"
	"sunset/data-hub/route/group"
	"sunset/data-hub/route/resource"
	"sunset/data-hub/route/statistics"
	"sunset/data-hub/route/task"
)

func InitCofnigHttpServer() {

	engin := gin.Default()
	//资源
	resource.RouteResource(engin)
	//分组
	group.RouteGroup(engin)
	//任务
	task.RouteTask(engin)
	//统计
	statistics.RouteGroup(engin)

	server := &http.Server{
		Handler: engin,
		Addr:    ":" + viper.GetString("port"),
	}
	err := server.ListenAndServe()
	if err != nil {
		panic(err)
	}
}
