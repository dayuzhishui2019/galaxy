package task

import (
	"github.com/gin-gonic/gin"
	"gopkg.in/mgo.v2/bson"
	"net/http"
	"strings"
	"sunset/data-hub/constants"
	"sunset/data-hub/db/mongo"
	"sunset/data-hub/model"
	"sunset/data-hub/route/common"
	"sunset/data-hub/util/uuid"
	"time"
)

func RouteTask(engine *gin.Engine) {
	group := engine.Group("/task")
	group.GET("/list", list)
	group.POST("/save", save)
	group.POST("/remove", remove)
}

func list(ctx *gin.Context) {
	var err error
	client, err := mongo.Dataset(constants.DB_DATASET_TASK)
	if err != nil {
		common.ResponseError(ctx, http.StatusInternalServerError, err)
		return
	}
	iter := client.Find(bson.M{
		"delFlag": bson.M{
			"$ne": model.TASK_DELETED_FLAG,
		},
	}).Iter()
	task := &model.Task{}
	tasks := make([]*model.Task, 0)
	for iter.Next(task) {
		tasks = append(tasks, task)
		task = &model.Task{}
	}
	if err := iter.Close(); err != nil {
		common.ResponseError(ctx, http.StatusInternalServerError, err)
		return
	}
	common.ResponseSuccess(ctx, tasks)
}

func save(ctx *gin.Context) {
	var err error
	client, err := mongo.Dataset(constants.DB_DATASET_TASK)
	if err != nil {
		common.ResponseError(ctx, http.StatusInternalServerError, err)
		return
	}
	task := &model.Task{}
	err = ctx.BindJSON(task)
	if err != nil {
		common.ResponseError(ctx, http.StatusBadRequest, err)
		return
	}
	if task.ID == "" {
		task.ID = uuid.UUIDShort()
		task.CreateTime = time.Now().UnixNano() / 1e6
	}
	task.UpdateTime = time.Now().UnixNano() / 1e6
	_, err = client.Upsert(bson.M{
		"id": task.ID,
	}, task)
	if err != nil {
		common.ResponseError(ctx, http.StatusInternalServerError, err)
		return
	}
	common.ResponseSuccess(ctx, task)
}

func remove(ctx *gin.Context) {
	var err error
	client, err := mongo.Dataset(constants.DB_DATASET_TASK)
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
	info, err := client.UpdateAll(bson.M{
		"id": bson.M{
			"$in": strings.Split(ids.Ids, ","),
		},
	}, bson.M{
		"$set": bson.M{
			"delFlag":    1,
			"updateTime": time.Now().UnixNano() / 1e6,
		},
	})
	if err != nil {
		common.ResponseError(ctx, http.StatusInternalServerError, err)
		return
	}
	common.ResponseSuccess(ctx, info.Removed)
}
