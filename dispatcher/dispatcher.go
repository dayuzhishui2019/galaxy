package dispatcher

import (
	bytes2 "bytes"
	"context"
	"errors"
	"fmt"
	"github.com/globalsign/mgo/bson"
	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/viper"
	"io/ioutil"
	"net/http"
	"sunset/data-hub/constants"
	"sunset/data-hub/db/mongo"
	"sunset/data-hub/logger"
	"sunset/data-hub/model"
	"sunset/data-hub/util"
	"sync"
	"time"
)

var TASK_TYPE_IMAGE = map[string]string{
	"1400server": "sunset/data-stream",
	"1400client": "sunset/data-stream",
}

var TASK_CONTAINER_PREFIX = "task_"

var client *http.Client

type TaskDispatcher struct {
	sync.Mutex
	Host        string
	ctx         context.Context
	cancel      context.CancelFunc
	taskMap     map[string]*model.Task
	taskBinding map[string]*Worker
	updateTime  int64
}

func (td *TaskDispatcher) Init() {
	client = &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        20,
			MaxIdleConnsPerHost: 5,
			MaxConnsPerHost:     5,
			IdleConnTimeout:     30 * time.Second,
		},
		Timeout: 3 * time.Second,
	}
	td.ctx, td.cancel = context.WithCancel(context.Background())
	td.taskMap = make(map[string]*model.Task)
	td.taskBinding = make(map[string]*Worker)
	go td.loopFindTask()
	go td.loopBindTask()
}
func (td *TaskDispatcher) loopFindTask() {
	for {
		time.Sleep(10 * time.Second)
		select {
		case <-td.ctx.Done():
			return
		default:
		}
		client, err := mongo.Dataset(constants.DB_DATASET_TASK)
		if err != nil {
			logger.LOG_WARN("获取mongo连接异常，", err)
			continue
		}
		tasks := make([]*model.Task, 0)
		pipeM := []bson.M{}
		pipeM = append(pipeM, bson.M{"$match": bson.M{"updateTime": bson.M{"$gt": td.updateTime}}})
		pipeM = append(pipeM, bson.M{"$sort": bson.M{"updateTime": -1}})
		err = client.Pipe(pipeM).All(&tasks)
		if err != nil {
			logger.LOG_WARN("查询最新任务失败，", err)
			continue
		}
		if len(tasks) == 0 {
			logger.LOG_WARN("无任务更新，", err)
			continue
		}
		if len(tasks) > 0 {
			logger.LOG_INFO("任务更新，数量：", len(tasks))
			td.Lock()
			for _, t := range tasks {
				if t.DelFlag == model.TASK_DELETED_FLAG {
					delete(td.taskMap, t.ID)
				} else {
					td.taskMap[t.ID] = t
				}
			}
			td.updateTime = tasks[0].UpdateTime
			td.Unlock()
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
				res, err := client.Post(fmt.Sprintf("http://%s:%s/mapi/init", w.td.Host, wt.ManagePort), "application/json", bytes2.NewReader(taskBytes))
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
			logger.LOG_INFO("keepalive:", fmt.Sprintf("http://%s:%s/mapi/keepAlive", w.td.Host, wt.ManagePort))
			res, err := client.Post(fmt.Sprintf("http://%s:%s/mapi/keepAlive", w.td.Host, wt.ManagePort), "text/plain", nil)
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
	img, ok := TASK_TYPE_IMAGE[task.TaskType]
	if !ok {
		logger.LOG_WARN("未找到任务类型对应的镜像，taskType:", task.TaskType)
		return
	}
	fmt.Println("启动进程：", w.task.Name)
	//stop container
	cmdRes, err := util.ExecCmd(fmt.Sprintf("docker stop %s", TASK_CONTAINER_PREFIX+task.ID))
	if err != nil {
		logger.LOG_WARN("关闭容器异常：", err)
	} else {
		logger.LOG_WARN("关闭容器成功：", cmdRes)
	}
	time.Sleep(5 * time.Second)
	//create container
	exportPorts := " -p " + task.ManagePort + ":" + task.ManagePort + " "
	if len(task.ExportPorts) > 0 {
		for _, p := range task.ExportPorts {
			exportPorts += " -p " + p + ":" + p + " "
		}
	}
	cmdRes, err = util.ExecCmd(fmt.Sprintf("docker run --rm -d %s --network app --name=%s -e MANAGE_PORT=%s -e HOST=%s -e LOG_LEVEL=%s -v /home/data-hub/logs/%s:/logs %s", exportPorts, TASK_CONTAINER_PREFIX+task.ID, task.ManagePort, viper.GetString("host"), viper.GetString("log.level"), "task_"+task.ID, img))
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
