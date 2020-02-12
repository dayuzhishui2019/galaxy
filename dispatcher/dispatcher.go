package dispatcher

import (
	bytes2 "bytes"
	"context"
	"dyzs/galaxy/constants"
	"dyzs/galaxy/logger"
	"dyzs/galaxy/model"
	"dyzs/galaxy/proxy"
	"dyzs/galaxy/redis"
	"dyzs/galaxy/util"
	"errors"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/viper"
	"io/ioutil"
	"net/http"
	"sync"
	"time"
)

var TASK_CONTAINER_PREFIX = "task_"

type TaskDispatcher struct {
	httpClient  *http.Client
	redisClient *redis.Cache

	sync.Mutex
	Host          string
	ctx           context.Context
	cancel        context.CancelFunc
	taskMap       map[string]*model.Task
	taskResources map[string][]*model.Resource
	taskBinding   map[string]*Worker
	updateTime    int64
}

func (td *TaskDispatcher) Init() {
	td.httpClient = &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        20,
			MaxIdleConnsPerHost: 5,
			MaxConnsPerHost:     5,
			IdleConnTimeout:     30 * time.Second,
		},
		Timeout: 3 * time.Second,
	}
	td.redisClient = redis.NewRedisCache(0, viper.GetString("redis.addr"), redis.FOREVER)
	td.ctx, td.cancel = context.WithCancel(context.Background())
	td.taskMap = make(map[string]*model.Task)
	td.taskBinding = make(map[string]*Worker)
	//从本地redis获取任务信息
	td.loadLocalTasks()
	//轮询更新任务
	go td.loopFindTask()
	//绑定任务
	go td.loopBindTask()
}

func (td *TaskDispatcher) loadLocalTasks() {
	localTasks := make([]*model.Task, 0)
	err := td.redisClient.StringGet(constants.REDIS_KEY_TASKS, &localTasks)
	if err != nil {
		logger.LOG_WARN("从redis获取任务失败", err)
		return
	}
	if len(localTasks) == 0 {
		logger.LOG_WARN("从redis获取任务0个")
		return
	}
	td.refreshTasks(localTasks)
}

func (td *TaskDispatcher) loopFindTask() {
	centerProxy := proxy.NewCenterProxy()
	for {
		time.Sleep(5 * time.Second)
		select {
		case <-td.ctx.Done():
			return
		default:
		}
		hr, err := centerProxy.Heart(td.getLocalTasks())
		if err != nil {
			logger.LOG_WARN("发送中心心跳请求失败，", err)
			continue
		}
		td.refreshTasks(hr.Tasks)
	}
}

func (td *TaskDispatcher) getLocalTasks() (localTasks []*model.Task) {
	td.Lock()
	for _, v := range td.taskMap {
		localTasks = append(localTasks, v)
	}
	td.Unlock()
	return localTasks
}

func (td *TaskDispatcher) refreshTasks(tasks []*model.Task) {
	if len(tasks) == 0 {
		logger.LOG_INFO("无任务更新")
		return
	}
	if len(tasks) > 0 {
		logger.LOG_INFO("任务更新，数量：", len(tasks))
		td.Lock()
		for _, t := range tasks {
			if t.Status == model.TASK_DELETED_FLAG {
				delete(td.taskMap, t.ID)
			} else {
				td.taskMap[t.ID] = t
			}
		}
		td.updateTime = tasks[0].UpdateTime
		td.Unlock()
		//save to redis
		var localTasks []*model.Task
		for _, v := range td.taskMap {
			localTasks = append(localTasks, v)
		}
		err := td.redisClient.StringSet(constants.REDIS_KEY_TASKS, localTasks)
		if err != nil {
			logger.LOG_ERROR("任务缓存入redis异常，", err)
		}
	}
}

func (td *TaskDispatcher) loopBindTask() {
	for {
		time.Sleep(3 * time.Second)
		select {
		case <-td.ctx.Done():
			return
		default:
			newTasks := make([]*model.Task, 0)
			//find new task
			td.Lock()
			for _, t := range td.taskMap {
				if td.taskBinding[t.ID] == nil {
					//add task
					logger.LOG_INFO("新增任务：", t.Name)
					newTasks = append(newTasks, t)
				} else if td.taskBinding[t.ID].task.UpdateTime != t.UpdateTime {
					//update task
					logger.LOG_INFO("更新任务：", t.Name)
					td.taskBinding[t.ID].task = t
				}
			}
			//bind task to worker
			for _, nt := range newTasks {
				worker := &Worker{
					td:   td,
					task: nt,
				}
				worker.start()
				td.taskBinding[nt.ID] = worker
			}
			td.Unlock()
		}
	}
}

type Worker struct {
	sync.Mutex
	td          *TaskDispatcher
	task        *model.Task
	workingTask *model.Task
	taskInited  bool

	ctx    context.Context
	cancel context.CancelFunc
}

func (w *Worker) start() {
	w.ctx, w.cancel = context.WithCancel(w.td.ctx)
	go w.bindTask()
	go w.keepaliveTask()
}

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
		_, ok := w.td.taskMap[w.task.ID]
		if !ok {
			w.td.Lock()
			delete(w.td.taskBinding, w.task.ID)
			w.td.Unlock()
			w.stopTask()
			w.cancel()
			return
		}
		//任务未创建
		var wt *model.Task
		w.Lock()
		wt = w.workingTask
		w.Unlock()
		if wt == nil {
			w.startTask()
		}
		w.Lock()
		wt = w.workingTask
		w.Unlock()
		//任务变更
		if wt != nil && wt.UpdateTime != w.task.UpdateTime {
			w.stopTask()
			time.Sleep(10 * time.Second)
			w.startTask()
		}
	}
}

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
		var taskInited bool
		w.Lock()
		wt = w.workingTask
		taskInited = w.taskInited
		w.Unlock()
		if wt == nil {
			continue
		}
		//初始化
		if !taskInited {
			taskBytes, err := jsoniter.Marshal(wt)
			if err != nil {
				logger.LOG_ERROR("任务转换json失败:", err)
				continue
			}
			err = util.Retry(func() error {
				res, err := w.td.httpClient.Post(fmt.Sprintf("http://%s:%s/mapi/init", TASK_CONTAINER_PREFIX+wt.ID, "7777"), "application/json", bytes2.NewReader(taskBytes))
				if err != nil {
					return err
				}
				defer func() {
					err := res.Body.Close()
					if err != nil {
						logger.LOG_WARN("关闭res失败", err)
					}
				}()
				bytes, err := ioutil.ReadAll(res.Body)
				if err != nil {
					return err
				}
				_ = res.Body.Close()
				if res.StatusCode != http.StatusOK {
					return errors.New(string(bytes))
				}
				return nil
			}, 3, 3*time.Second)
			if err != nil {
				logger.LOG_INFO("任务init异常，", err)
			} else {
				w.Lock()
				w.taskInited = true
				w.Unlock()
			}
		}
		//keep alive
		err := util.Retry(func() error {
			logger.LOG_INFO("keepalive:", fmt.Sprintf("http://%s:%s/mapi/keepAlive", TASK_CONTAINER_PREFIX+wt.ID, "7777"))
			res, err := w.td.httpClient.Post(fmt.Sprintf("http://%s:%s/mapi/keepAlive", TASK_CONTAINER_PREFIX+wt.ID, "7777"), "text/plain", nil)
			if err != nil {
				return err
			}
			defer func() {
				err := res.Body.Close()
				if err != nil {
					logger.LOG_WARN("关闭res失败", err)
				}
			}()
			bytes, err := ioutil.ReadAll(res.Body)
			if err != nil {
				return err
			}
			_ = res.Body.Close()
			if res.StatusCode != http.StatusOK {
				return errors.New(string(bytes))
			}
			return nil
		}, 3, 3*time.Second)
		if err != nil {
			logger.LOG_INFO("任务keep-alive异常，", err)
			logger.LOG_INFO("重启任务:", w.task.ID)
			w.startTask()
		}
	}
}

func (w *Worker) startTask() {
	w.Lock()
	w.taskInited = false
	w.Unlock()
	task := w.task
	if task.Repository == "" {
		logger.LOG_WARN("未找到任务类型对应的镜像，taskType:", task.AccessType)
		return
	}
	logger.LOG_WARN("启动进程：", w.task.Name)
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
	var cmd bytes2.Buffer
	cmd.WriteString("docker run --rm -d ")
	//ports
	if len(task.ExportPorts) > 0 {
		for _, p := range task.ExportPorts {
			cmd.WriteString(" -p " + p + ":" + p + " ")
		}
	}
	//network
	cmd.WriteString(" --network app ")
	//name
	cmd.WriteString(" --name=" + taskDir)
	//env
	cmd.WriteString(" -e MANAGE_PORT=7777 ")
	cmd.WriteString(" -e HOST=" + viper.GetString("host") + " ")
	cmd.WriteString(" -e LOG_LEVEL=" + viper.GetString("log.level") + " ")
	//volume
	cmd.WriteString(" -v /home/data-hub/logs/" + taskDir + ":/logs ")
	//image
	cmd.WriteString(img)

	cmdRes, err = util.ExecCmd(cmd.String())
	if err != nil {
		logger.LOG_WARN("启动容器异常：", err)
		return
	}
	logger.LOG_WARN("启动容器成功：", cmdRes)
	w.Lock()
	w.workingTask = w.task
	w.Unlock()
}

func (w *Worker) stopTask() {
	wt := w.workingTask
	if wt != nil {
		//stop container
		cmdRes, err := util.ExecCmd(fmt.Sprintf("docker stop %s", TASK_CONTAINER_PREFIX+wt.ID))
		if err != nil {
			logger.LOG_WARN("关闭容器异常：", err)
		} else {
			logger.LOG_WARN("关闭容器成功：", cmdRes)
		}
	}
}
