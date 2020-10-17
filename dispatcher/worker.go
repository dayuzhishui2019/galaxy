package dispatcher

import (
	"bytes"
	"context"
	"dyzs/galaxy/logger"
	"dyzs/galaxy/model"
	"dyzs/galaxy/util"
	"errors"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/viper"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

var managePortPool = make(map[int]bool)
var managePortStart = 32000
var managePortPoolLock sync.Mutex

var taskResources = make(map[string]map[string]bool)
var taskManagePort = make(map[string]int)

const (
	_URL_INIT            = "http://%s:%s/mapi/init"
	_URL_HEART           = "http://%s:%s/mapi/heart"
	_URL_ASSIGN_RESOURCE = "http://%s:%s/mapi/assignResource"
	_URL_REVOKE_RESOURCE = "http://%s:%s/mapi/revokeResource"
)

//任务执行器
type Worker struct {
	sync.Mutex
	td *TaskDispatcher

	TaskId      string
	workingTask *model.Task
	taskInited  bool
	managePort  int

	ctx    context.Context
	cancel context.CancelFunc
}

func GetTaskAddressByResourceId(resourceId string) (taskAddress string) {

	//taskResources["gbaccess"] = map[string]bool{"34020000001320000001": true, "34020000001110000001": true}
	//taskResources["onviftask"] = map[string]bool{"34020000001320000001":true}

	var taskIds []string
	for taskId, resourceIds := range taskResources {
		if _, ok := resourceIds[resourceId]; ok {
			taskIds = append(taskIds, taskId)
		}
	}
	if len(taskIds) == 0 {
		return ""
	}
	if len(taskIds) > 1 {
		logger.LOG_WARN("资源下发到了多个任务，资源ID：", resourceId, ";任务Ids：", taskIds)
	}
	return TASK_CONTAINER_PREFIX + taskIds[0] + ":" + strconv.Itoa(taskManagePort[taskIds[0]]) //+ ":8000" //
}

//执行器启动
func (w *Worker) start() {
	//test
	//newTask := w.td.GetTaskById(w.TaskId)
	//if newTask == nil {
	//	return
	//}
	//newTask.ID = "gbaccess"
	//w.managePort = 8000
	//
	////go func(){
	////	for{
	//err := w.initTask(newTask)
	//fmt.Println(err)
	////		time.Sleep(time.Second*30)
	////	}
	////}()
	//return

	w.ctx, w.cancel = context.WithCancel(w.td.ctx)
	w.managePort = getNewManagePort()
	taskManagePort[w.TaskId] = w.managePort
	go w.bindTask()
	go w.keepaliveTask()
}

//监测任务绑定状态
func (w *Worker) bindTask() {
	for {
		time.Sleep(5 * time.Second)
		select {
		//分发器停止，停止进程
		case <-w.ctx.Done():
			w.stopTask()
			return
		default:
		}
		//任务取消，停止进程
		newTask := w.td.GetTaskById(w.TaskId)
		if newTask == nil {
			w.td.ReleaseTask(w.TaskId)
			w.stopTask()
			w.cancel()
			return
		}
		//任务未创建
		var wt *model.Task
		w.Lock()
		wt = w.workingTask
		w.Unlock()
		//任务组件变更/端口变更
		if wt == nil || wt.Repository != newTask.Repository || wt.CurrentTag != newTask.CurrentTag || !ComparePorts(wt.ExportPorts, newTask.ExportPorts) {
			w.startTask(newTask)
		}
		w.Lock()
		wt = w.workingTask
		w.Unlock()
		if wt == nil {
			//创建失败，重新创建
			continue
		}
		//初始化
		var taskInited bool
		w.Lock()
		taskInited = w.taskInited
		w.Unlock()
		if !taskInited {
			err := w.initTask(newTask)
			if err != nil {
				//初始化失败，重新初始化
				logger.LOG_WARN("任务init异常，", err)
				//w.workingTask = nil
				continue
			} else {
				w.Lock()
				w.taskInited = true
				w.Unlock()
			}
		}
		//任务无变更
		if wt.UpdateTime == newTask.UpdateTime && wt.ResourceId == newTask.ResourceId {
			continue
		}
		var err error
		//任务配置变更
		if wt.UpdateTime != newTask.UpdateTime && !compareTask(wt, newTask) {
			err = w.initTask(newTask)
			if err != nil {
				logger.LOG_WARN("更新任务配置异常：", err)
				continue
			}
		}
		//任务资源变更
		if wt.ResourceId != newTask.ResourceId {
			err = w.refreshResource(wt, newTask)
			if err != nil {
				logger.LOG_WARN("更新任务资源异常：", err)
				continue
			}
		}
		//更新完成
		if err == nil {
			w.Lock()
			w.workingTask = newTask
			w.Unlock()
		}
	}
}

func ComparePorts(a, b string) bool {
	a = strings.Trim(a, " ")
	b = strings.Trim(b, " ")
	if a == b {
		return true
	}
	aeps := make([]string, 0)
	beps := make([]string, 0)
	err := jsoniter.Unmarshal([]byte(a), &aeps)
	if err != nil {
		logger.LOG_WARN("端口映射解析异常：", a, ",ERR:", err)
		return true
	}
	err = jsoniter.Unmarshal([]byte(b), &beps)
	if err != nil {
		logger.LOG_WARN("端口映射解析异常：", b, ",ERR:", err)
		return true
	}
	if len(aeps) != len(beps) {
		return false
	}
	if (aeps == nil) != (beps == nil) {
		return false
	}
	mapA := make(map[string]string, 0)
	for _, value := range aeps {
		mapA[value] = value
	}
	for _, value := range beps {
		if _, ok := mapA[value]; !ok {
			return false
		}
	}
	return true
}

//初始化任务
func (w *Worker) initTask(task *model.Task) error {
	copyTask := &model.Task{
		ID:            task.ID,
		Name:          task.Name,
		Repository:    task.Repository,
		PreviousTag:   task.PreviousTag,
		CurrentTag:    task.CurrentTag,
		AccessType:    task.AccessType,
		ExportPorts:   task.ExportPorts,
		AllResource:   task.AllResource,
		ResourceId:    task.ResourceId,
		Status:        task.Status,
		AccessParam:   task.AccessParam,
		CreateTime:    task.CreateTime,
		UpdateTime:    task.UpdateTime,
		NodeID:        task.NodeID,
		ResourceBytes: "",
	}
	err := request(fmt.Sprintf(_URL_INIT, TASK_CONTAINER_PREFIX+task.ID, strconv.Itoa(w.managePort)), http.MethodPost, "application/json", copyTask, nil)
	if err != nil {
		return err
	}
	return w.refreshResource(nil, task)
}

//刷新资源
func (w *Worker) refreshResource(oldTask, newTask *model.Task) error {
	var oldResources []*model.Resource
	var newResourceIds = make(map[string]bool)
	if oldTask != nil {
		oldResources = oldTask.GetResources()
	} else {
		oldResources = make([]*model.Resource, 0)
	}
	newResources := newTask.GetResources()
	oldResourceMap := make(map[string]*model.Resource)
	var removeR []string
	var updateR = make([]*model.Resource, 0)
	var addR = make([]*model.Resource, 0)
	//删除
	if len(oldResources) > 0 {
		newResourceMap := make(map[string]*model.Resource)
		for _, r := range newResources {
			newResourceMap[r.ID] = r
		}
		for _, r := range oldResources {
			oldResourceMap[r.ID] = r
			nr, ok := newResourceMap[r.ID]
			//删除
			if !ok {
				removeR = append(removeR, r.ID)
			} else if !compareResource(r, nr) {
				//更新
				updateR = append(updateR, nr)
			}
		}
	}
	//新增
	for _, r := range newResources {
		newResourceIds[r.ID] = true
		newResourceIds[r.GbID] = true
		_, ok := oldResourceMap[r.ID]
		if !ok {
			addR = append(addR, r)
		}
	}
	//request remove
	if len(removeR) > 0 {
		logger.LOG_WARN("revoke resource，task【", w.TaskId, "】,count：", len(removeR))
		err := request(fmt.Sprintf(_URL_REVOKE_RESOURCE, TASK_CONTAINER_PREFIX+w.TaskId, strconv.Itoa(w.managePort)), http.MethodPost, "application/json", removeR, nil)
		if err != nil {
			return err
		}
		logger.LOG_WARN("revoke resource success")
	}
	//request add
	addR = append(addR, updateR...)
	if len(addR) > 0 {
		logger.LOG_WARN("assign resource，task【", w.TaskId, "】,count：", len(addR))
		err := request(fmt.Sprintf(_URL_ASSIGN_RESOURCE, TASK_CONTAINER_PREFIX+w.TaskId, strconv.Itoa(w.managePort)), http.MethodPost, "application/json", addR, nil)
		if err != nil {
			return err
		}
		logger.LOG_WARN("assign resource success")
	}

	//缓存任务资源关系
	taskResources[newTask.ID] = newResourceIds
	return nil
}

//比对任务
func compareTask(a, b *model.Task) bool {
	return a.AccessType == b.AccessType && a.AccessParam == b.AccessParam
}

//比对资源
func compareResource(a, b *model.Resource) bool {
	return a.GbID == b.GbID && a.DominionCode == b.DominionCode && a.MvcIP == b.MvcIP && a.MvcPort == b.MvcPort && a.MvcUsername == b.MvcUsername && a.MvcPassword == b.MvcPassword && a.MvcChannels == b.MvcChannels
}

//任务保活
func (w *Worker) keepaliveTask() {
	for {
		time.Sleep(5 * time.Second)
		select {
		//分发器停止，停止进程
		case <-w.ctx.Done():
			return
		default:
		}
		var wt *model.Task
		w.Lock()
		wt = w.workingTask
		w.Unlock()
		if wt == nil {
			continue
		}
		if !w.taskInited {
			continue
		}
		//keep alive
		err := request(fmt.Sprintf(_URL_HEART, TASK_CONTAINER_PREFIX+wt.ID, strconv.Itoa(w.managePort)), http.MethodPost, "application/json", map[string]interface{}{}, nil)
		if err != nil {
			logger.LOG_WARN("任务keep-alive异常，", err)
			logger.LOG_WARN("关闭任务:", w.TaskId)
			w.stopTask()
		}
	}
}

//启动任务
func (w *Worker) startTask(task *model.Task) {
	w.Lock()
	w.taskInited = false
	w.Unlock()
	//stop container
	w.stopTask()
	//启动
	if task.Repository == "" {
		logger.LOG_WARN("未找到任务类型对应的镜像，taskType:", task.AccessType)
		return
	}
	logger.LOG_WARN("启动进程：", task.Name)
	//stop container
	cmdRes, err := util.ExecCmd(fmt.Sprintf("docker stop %s", TASK_CONTAINER_PREFIX+task.ID))
	if err != nil {
		logger.LOG_WARN("关闭容器异常：", err)
	} else {
		logger.LOG_WARN("关闭容器成功：", cmdRes)
	}
	time.Sleep(5 * time.Second)
	//create container
	img := task.Repository
	if task.CurrentTag != "" {
		img += ":" + task.CurrentTag
	}
	taskDir := TASK_CONTAINER_PREFIX + task.ID
	var cmd bytes.Buffer
	cmd.WriteString("docker run --rm -d ")
	//ports
	if len(task.ExportPorts) > 0 {
		eps := make([]string, 0)
		err := jsoniter.Unmarshal([]byte(task.ExportPorts), &eps)
		if err != nil {
			logger.LOG_WARN("端口映射解析异常：", task.ID, task.ExportPorts, ",ERR:", err)
			return
		}
		for _, p := range eps {
			_, err := strconv.Atoi(strings.Trim(p, " "))
			if err == nil {
				cmd.WriteString(" -p " + p + ":" + p + " -p " + p + ":" + p + "/udp ")
			}
		}
	}
	//network
	cmd.WriteString(" --network app ")
	//name
	cmd.WriteString(" --name=" + taskDir)
	//env
	cmd.WriteString(" -e GALAXY_IP=" + viper.GetString("center.host") + " ")
	cmd.WriteString(" -e GALAXY_PORT=" + viper.GetString("port") + " ")
	cmd.WriteString(" -e MANAGE_PORT=" + strconv.Itoa(w.managePort) + " ")
	cmd.WriteString(" -e HOST=" + viper.GetString("host") + " ")
	cmd.WriteString(" -e LOG_LEVEL=" + viper.GetString("log.level") + " ")
	cmd.WriteString(" -e CENTER_IP=" + viper.GetString("center.host") + " ")
	cmd.WriteString(" -e CENTER_PORT=" + viper.GetString("center.managePort") + " ")
	//volume
	cmd.WriteString(" -v /home/dyzs/logs/" + taskDir + ":/logs ")
	//image
	cmd.WriteString(img)

	cmdRes, err = util.ExecCmd(cmd.String())
	if err != nil {
		logger.LOG_WARN("启动容器异常：", err)
		return
	}
	logger.LOG_WARN("启动容器成功：", cmdRes)
	w.Lock()
	w.workingTask = task
	w.Unlock()
}

//停止任务
func (w *Worker) stopTask() {
	//stop container
	cmdRes, err := util.ExecCmd(fmt.Sprintf("docker stop %s", TASK_CONTAINER_PREFIX+w.TaskId))
	if err != nil {
		logger.LOG_WARN("关闭容器异常：", err)
	} else {
		logger.LOG_WARN("关闭容器成功：", cmdRes)
	}
	_, _ = util.ExecCmd(fmt.Sprintf("docker rm %s", TASK_CONTAINER_PREFIX+w.TaskId))
	w.Lock()
	w.workingTask = nil
	w.Unlock()
}

func getNewManagePort() (port int) {
	managePortPoolLock.Lock()
	managePortPoolLock.Unlock()
	for i := managePortStart; i < 65535; i++ {
		if used, _ := managePortPool[i]; !used {
			managePortPool[i] = true
			return i
		}
	}
	return -1
}

func revokeManagePort(port int) {
	managePortPoolLock.Lock()
	managePortPoolLock.Unlock()
	delete(managePortPool, port)
}

var workerHttpClient = &http.Client{
	Transport: &http.Transport{
		MaxIdleConns:        20,
		MaxIdleConnsPerHost: 5,
		MaxConnsPerHost:     5,
		IdleConnTimeout:     30 * time.Second,
	},
	Timeout: 3 * time.Second,
}

//http请求
func request(url, method, contentType string, body interface{}, resPointer interface{}) error {
	var bodyBytes []byte
	var resBytes []byte
	if body != nil {
		bodyBytes, _ = jsoniter.Marshal(body)
	}
	logger.LOG_INFO("http-request:", url)
	if logger.IsDebug() {
		params, err := jsoniter.Marshal(body)
		if err != nil {
			logger.LOG_WARN("http-request-params-error:", err)
		} else {
			logger.LOG_INFO("http-request-params:", string(params))
		}
	}
	err := util.Retry(func() error {
		req, err := http.NewRequest(method, url, bytes.NewReader(bodyBytes))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", contentType)
		res, err := workerHttpClient.Do(req)
		if err != nil {
			return err
		}
		defer func() {
			err := res.Body.Close()
			if err != nil {
				logger.LOG_WARN("关闭res失败", err)
			}
		}()
		resBytes, err = ioutil.ReadAll(res.Body)
		if err != nil {
			return err
		}
		_ = res.Body.Close()
		if res.StatusCode != http.StatusOK {
			return errors.New(string(resBytes))
		}
		return nil
	}, 3, 3*time.Second)
	if err != nil {
		return err
	}
	res := &ResponseWrap{}
	err = jsoniter.Unmarshal(resBytes, res)
	if err != nil {
		return err
	}
	if res.Code != http.StatusOK {
		return errors.New("error response code:" + strconv.Itoa(res.Code))
	}
	if resPointer != nil {
		return jsoniter.Unmarshal(res.Msg, resPointer)
	}
	return nil
}

type ResponseWrap struct {
	Code int                 `json:"code"`
	Msg  jsoniter.RawMessage `json:"msg"`
}
