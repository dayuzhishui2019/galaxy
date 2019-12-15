package statistics

import (
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"net/http"
	"sunset/data-hub/redis"
	"sunset/data-hub/route/common"
)

var _REDIS_KEY_STATISTICS = "data-hub-statistics"

func RouteGroup(engine *gin.Engine) {
	engine.GET("/statistics", statistics)
}

var redisClient *redis.Cache

func statistics(ctx *gin.Context) {
	if redisClient == nil {
		redisClient = redis.NewRedisCache(0, viper.GetString("redis.addr"), redis.FOREVER)
	}
	var raws interface{}
	err := redisClient.StringGet(_REDIS_KEY_STATISTICS, &raws)
	if err != nil {
		common.ResponseError(ctx, http.StatusInternalServerError, err)
		return
	}
	common.ResponseSuccess(ctx, raws)
}
