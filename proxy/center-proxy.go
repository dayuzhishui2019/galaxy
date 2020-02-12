package proxy

import (
	"bytes"
	"dyzs/galaxy/concurrent"
	"dyzs/galaxy/logger"
	"dyzs/galaxy/model"
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
	address := viper.GetString("center.address")
	urlHeart := viper.GetString("center.url-heart")
	if address == "" {
		return nil, errors.New("配置中心地址为空")
	}
	if urlHeart == "" {
		return nil, errors.New("配置中心心跳接口地址为空")
	}
	hcp.address = address

	var lastUpdateTime int64
	localTaskMap := make(map[string]*model.Task)
	for _, v := range localTasks {
		localTaskMap[v.ID] = v
		if v.UpdateTime > lastUpdateTime {
			lastUpdateTime = v.UpdateTime
		}
	}
	res, err := hcp.client.Post(hcp.address+urlHeart+"?taskUpdateTime="+strconv.FormatInt(lastUpdateTime, 10), "application/json", hcp.generateHeartRequest())
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
		oldT, ok := localTaskMap[t.ID]
		if len(t.ResourceId) > 0 && (!ok || t.ResourceId != oldT.ResourceId || len(oldT.ResourceBytes) == 0) {
			unloadResourceTask = append(unloadResourceTask, t)
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
				res, err := hcp.client.Get(hcp.address + urlResource + "/" + task.ResourceId)
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
				task.ResourceBytes = resBytes
			})
		}(t)
	}
	err := hcp.executor.SubmitSyncBatch(requests)
	if err != nil {
		logger.LOG_WARN("请求任务资源异常", err)
	}
}
