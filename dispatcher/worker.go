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

//任务执行器
type Worker struct {
	sync.Mutex
	td          *TaskDispatcher
	task        *model.Task
	workingTask *model.Task
	taskInited  bool

	ctx    context.Context
	cancel context.CancelFunc
}

//执行器启动
func (w *Worker) start() {
	w.ctx, w.cancel = context.WithCancel(w.td.ctx)
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
				res, err := w.td.httpClient.Post(fmt.Sprintf("http://%s:%s/mapi/init", TASK_CONTAINER_PREFIX+wt.ID, "7777"), "application/json", bytes.NewReader(taskBytes))
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
			logger.LOG_INFO("keepalive:", fmt.Sprintf("http://%s:%s/mapi/heart", TASK_CONTAINER_PREFIX+wt.ID, "7777"))
			res, err := w.td.httpClient.Post(fmt.Sprintf("http://%s:%s/mapi/heart", TASK_CONTAINER_PREFIX+wt.ID, "7777"), "text/plain", nil)
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

//启动任务
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
	var cmd bytes.Buffer
	cmd.WriteString("docker run --rm -d ")
	//ports
	if len(task.ExportPorts) > 0 {
		eps := make([]string, 0)
		err := jsoniter.Unmarshal([]byte(task.ExportPorts), &eps)
		if err != nil {
			logger.LOG_WARN("端口映射解析异常：", err)
			return
		}
		for _, p := range eps {
			_, err := strconv.Atoi(strings.Trim(p, " "))
			if err == nil {
				cmd.WriteString(" -p " + p + ":" + p + " ")
			}
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
	w.workingTask = w.task
	w.Unlock()
}

//停止任务
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
