package group

import (
	"github.com/gin-gonic/gin"
	"gopkg.in/mgo.v2/bson"
	"net/http"
	"strings"
	"sunset/data-hub/constants"
	"sunset/data-hub/db/mongo"
	"sunset/data-hub/model"
	"sunset/data-hub/route/common"
)

func RouteGroup(engine *gin.Engine) {
	group := engine.Group("/group")
	group.GET("/list", list)
	group.POST("/save", save)
	group.POST("/remove", remove)
}

func list(ctx *gin.Context) {
	var err error
	client, err := mongo.Dataset(constants.DB_DATASET_GROUP)
	if err != nil {
		common.ResponseError(ctx, http.StatusInternalServerError, err)
		return
	}
	iter := client.Find(bson.M{}).Iter()
	eq := &model.Group{}
	eqs := make([]*model.Group, 0)
	for iter.Next(eq) {
		eqs = append(eqs, eq)
		eq = &model.Group{}
	}
	if err := iter.Close(); err != nil {
		common.ResponseError(ctx, http.StatusInternalServerError, err)
		return
	}
	common.ResponseSuccess(ctx, eqs)
}

func save(ctx *gin.Context) {
	var err error
	client, err := mongo.Dataset(constants.DB_DATASET_GROUP)
	if err != nil {
		common.ResponseError(ctx, http.StatusInternalServerError, err)
		return
	}
	eq := &model.Group{}
	err = ctx.BindJSON(eq)
	if err != nil {
		common.ResponseError(ctx, http.StatusBadRequest, err)
		return
	}
	if eq.ID == "" {
		eq.ID = eq.GbID
	}
	_, err = client.Upsert(bson.M{
		"id": eq.ID,
	}, eq)
	if err != nil {
		common.ResponseError(ctx, http.StatusInternalServerError, err)
		return
	}
	common.ResponseSuccess(ctx, eq)
}

func remove(ctx *gin.Context) {
	var err error
	client, err := mongo.Dataset(constants.DB_DATASET_GROUP)
	if err != nil {
		common.ResponseError(ctx, http.StatusInternalServerError, err)
		return
	}
	ids := &common.IdsParams{}
	err = ctx.BindJSON(ids)
	if err != nil {
		common.ResponseError(ctx, http.StatusBadRequest, err)
		return
	}
	info, err := client.RemoveAll(bson.M{
		"id": bson.M{
			"$in": strings.Split(ids.Ids, ","),
		},
	})
	if err != nil {
		common.ResponseError(ctx, http.StatusInternalServerError, err)
		return
	}
	common.ResponseSuccess(ctx, info.Removed)
}
