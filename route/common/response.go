package common

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

type res struct {
	Code int         `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data"`
}

func ResponseSuccess(ctx *gin.Context, data interface{}) {
	ctx.JSON(http.StatusOK, res{
		Code: http.StatusOK,
		Msg:  "ok",
		Data: data,
	})
}

func ResponseError(ctx *gin.Context, code int, err error) {
	ctx.JSON(code, res{
		Code: code,
		Msg:  err.Error(),
		Data: struct{}{},
	})
}
