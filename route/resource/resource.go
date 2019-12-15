package resource

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/globalsign/mgo/bson"
	"net/http"
	"strings"
	"sunset/data-hub/constants"
	"sunset/data-hub/db/mongo"
	"sunset/data-hub/model"
	"sunset/data-hub/route/common"
	"time"
)


func RouteResource(engine *gin.Engine) {
	group := engine.Group("/resource")
	group.POST("/list", list)
	group.POST("/save", save)
	group.POST("/batchSave", batchSave)
	group.POST("/remove", remove)
}

type SearchCondition struct {
	common.Page
	Name string `json:"name"`
	GbID string `json:"gbId"`
	Type string `json:"type"`
}

func list(ctx *gin.Context) {
	var err error
	condition := &SearchCondition{}
	err = ctx.BindJSON(condition)
	client, err := mongo.Dataset(constants.DB_DATASET_RESOURCE)
	if err != nil {
		common.ResponseError(ctx, http.StatusInternalServerError, err)
		return
	}
	eqs := make([]*model.Resource, 0)
	pipeM := []bson.M{}
	query := bson.M{}
	if condition.GbID != "" {
		query["gbId"] = condition.GbID
	}
	if condition.Name != "" {
		query["name"] = bson.M{"$regex": bson.RegEx{condition.Name, "i"}}
	}
	if condition.Type != "" {
		query["type"] = condition.Type
	}
	pipeM = append(pipeM, bson.M{"$match": query})
	pipeM = append(pipeM, bson.M{"$sort": bson.M{"gbId": 1}})
	pipeM = append(pipeM, bson.M{"$skip": condition.GetStart()})
	pipeM = append(pipeM, bson.M{"$limit": condition.PageSize})
	err = client.Pipe(pipeM).All(&eqs)
	count, err := client.Count()
	if err != nil {
		common.ResponseError(ctx, http.StatusInternalServerError, err)
		return
	}
	common.ResponseSuccess(ctx, map[string]interface{}{
		"total": count,
		"list":  eqs,
	})
}

func save(ctx *gin.Context) {
	var err error
	client, err := mongo.Dataset(constants.DB_DATASET_RESOURCE)
	if err != nil {
		common.ResponseError(ctx, http.StatusInternalServerError, err)
		return
	}
	eq := &model.Resource{}
	err = ctx.BindJSON(eq)
	if err != nil {
		common.ResponseError(ctx, http.StatusBadRequest, err)
		return
	}
	if eq.ID == "" {
		eq.ID = eq.GbID
	}
	eq.UpdateTime = time.Now().UnixNano() / 1e6
	_, err = client.Upsert(bson.M{
		"id": eq.ID,
	}, eq)
	if err != nil {
		common.ResponseError(ctx, http.StatusInternalServerError, err)
		return
	}
	common.ResponseSuccess(ctx, eq)
}

func batchSave(ctx *gin.Context) {
	var err error
	client, err := mongo.Dataset(constants.DB_DATASET_RESOURCE)
	if err != nil {
		common.ResponseError(ctx, http.StatusInternalServerError, err)
		return
	}
	resources := &struct {
		Resources []*model.Resource `json:"resources"`
	}{}
	err = ctx.BindJSON(resources)
	if err != nil || len(resources.Resources) == 0 {
		common.ResponseError(ctx, http.StatusBadRequest, err)
		return
	}
	start := time.Now()
	var success int
	for _, item := range resources.Resources {
		item.UpdateTime = time.Now().UnixNano() / 1e6
		_, err = client.Upsert(bson.M{
			"id": item.ID,
		}, item)
		if err == nil {
			success++
		}
	}
	fmt.Println("耗时：", time.Since(start))
	if err != nil {
		common.ResponseError(ctx, http.StatusInternalServerError, err)
		return
	}
	common.ResponseSuccess(ctx, success)
}

func remove(ctx *gin.Context) {
	var err error
	client, err := mongo.Dataset(constants.DB_DATASET_RESOURCE)
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
