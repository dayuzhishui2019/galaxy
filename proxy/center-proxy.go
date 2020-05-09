package proxy

import (
	"bytes"
	"dyzs/galaxy/concurrent"
	"dyzs/galaxy/constants"
	"dyzs/galaxy/logger"
	"dyzs/galaxy/model"
	"dyzs/galaxy/redis"
	"encoding/json"
	"errors"
	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/viper"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"
)

type HttpCenterProxy struct {
	address  string
	client   *http.Client
	executor *concurrent.Executor

	localCenterHost string
}

type HttpResponseWrapper struct {
	Code    string          `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

func (hcp *HttpCenterProxy) Init() {
	hcp.client = &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        20,
			MaxIdleConnsPerHost: 5,
			MaxConnsPerHost:     5,
			IdleConnTimeout:     30 * time.Second,
		},
		Timeout: 3 * time.Second,
	}
	hcp.executor = concurrent.NewExecutor(10)
}

func (hcp *HttpCenterProxy) Heart(localTasks []*model.Task) (hr *HeartResonse, err error) {
	host := viper.GetString("center.host")
	managePort := viper.GetString("center.managePort")
	urlHeart := viper.GetString("center.url-heart")
	if host == "" {
		return nil, errors.New("配置中心地址为空")
	}
	if managePort == "" {
		return nil, errors.New("配置中心管理端口为空")
	}
	if urlHeart == "" {
		return nil, errors.New("配置中心心跳接口地址为空")
	}
	if hcp.localCenterHost != host {
		redisClient := redis.NewRedisCache(0, viper.GetString("redis.addr"), redis.FOREVER)
		err = redisClient.StringSet(constants.REDIS_KEY_CENTERHOST, host)
		if err != nil {
			logger.LOG_ERROR("BoxId缓存入redis异常，", err)
		} else {
			hcp.localCenterHost = host
		}
		_ = redisClient.Close()
	}
	hcp.address = host + ":" + managePort
	var lastUpdateTime int64
	localTaskMap := make(map[string]*model.Task)
	for _, v := range localTasks {
		localTaskMap[v.ID] = v
		if v.UpdateTime > lastUpdateTime {
			lastUpdateTime = v.UpdateTime
		}
	}
	url := "http://"+hcp.address+urlHeart+"?time="+strconv.FormatInt(lastUpdateTime, 10)
	logger.LOG_INFO("heart-request:",url)
	res, err := hcp.client.Post(url, "application/json", hcp.generateHeartRequest())
	if err != nil {
		return nil, err
	}
	defer func() {
		err := res.Body.Close()
		if err != nil {
			logger.LOG_WARN("关闭res失败", err)
		}
	}()
	resBytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	_ = res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, errors.New(string(resBytes))
	}
	wrap := &HttpResponseWrapper{}
	hr = &HeartResonse{}
	err = jsoniter.Unmarshal(resBytes, wrap)
	if err != nil {
		return nil, err
	}
	err = jsoniter.Unmarshal(wrap.Data, hr)
	if err != nil {
		return nil, err
	}
	//请求资源（未获取过得和变更的）
	var unloadResourceTask []*model.Task
	for _, t := range hr.Tasks {
		t.NodeID = hr.Node.Id

		rrr, _ := jsoniter.Marshal(t)
		logger.LOG_INFO("task:",string(rrr))

		oldT, ok := localTaskMap[t.ID]
		if len(t.ResourceId) > 0 && (!ok || t.ResourceId != oldT.ResourceId || len(oldT.ResourceBytes) == 0) {
			unloadResourceTask = append(unloadResourceTask, t)
		}
		if ok && t.ResourceId == oldT.ResourceId && len(oldT.ResourceBytes) > 0 {
			t.ResourceBytes = oldT.ResourceBytes
		}
	}
	if len(unloadResourceTask) > 0 {
		hcp.loadResourcesOfTasks(unloadResourceTask)
	}
	return hr, nil
}

func (hcp *HttpCenterProxy) generateHeartRequest() io.Reader {
	m := make(map[string]string)
	m["serialNumber"] = viper.GetString("sn")
	m["model"] = viper.GetString("model")
	m["name"] = viper.GetString("name")
	b, err := json.Marshal(m)
	if err != nil {
		logger.LOG_WARN(err)
	}
	return bytes.NewReader(b)
}

//获取任务的资源集合
func (hcp *HttpCenterProxy) loadResourcesOfTasks(tasks []*model.Task) {
	urlResource := viper.GetString("center.url-resource")
	var requests []func()
	for _, t := range tasks {
		func(task *model.Task) {
			requests = append(requests, func() {
				res, err := hcp.client.Get("http://" + hcp.address + urlResource + "/" + task.ResourceId)
				if err != nil {
					logger.LOG_WARN("获取任务资源异常，", err)
					return
				}
				if res.StatusCode != 200 {
					logger.LOG_WARN("获取任务资源异常，code:", res.StatusCode)
					return
				}
				defer func() {
					err := res.Body.Close()
					if err != nil {
						logger.LOG_WARN("关闭res失败", err)
					}
				}()
				resBytes, err := ioutil.ReadAll(res.Body)
				if err != nil {
					return
				}
				task.ResourceBytes = string(resBytes)
			})
		}(t)
	}
	err := hcp.executor.SubmitSyncBatch(requests)
	if err != nil {
		logger.LOG_WARN("请求任务资源异常", err)
	}
}
