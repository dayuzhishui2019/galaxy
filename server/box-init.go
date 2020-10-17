package server

import (
	"github.com/gin-gonic/gin"
	"net/http"
	"strings"
)

func RouteBoxInit(engine *gin.Engine) {
	engine.GET("/init", Init)
}

func Init(context *gin.Context) {
	context.HTML(http.StatusOK, "init.tmpl", gin.H{
		"title": "hello gin " + strings.ToLower(context.Request.Method) + " method",
	})
}
