package dispatcher

import (
	"bytes"
	"context"
	"dyzs/galaxy/constants"
	"dyzs/galaxy/logger"
	"dyzs/galaxy/model"
	"dyzs/galaxy/proxy"
	"dyzs/galaxy/redis"
	"github.com/spf13/viper"
	"net/http"
	"sync"
	"time"
)

var TASK_CONTAINER_PREFIX = "task_"
var _DEFAULT_HEART_INTERVAL = 30 //默认心跳间隔30s

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
}

//初始化
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

//加载本地任务列表
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

//轮询变更任务
func (td *TaskDispatcher) loopFindTask() {
	centerProxy := proxy.NewCenterProxy()
	heartInterval := viper.GetInt("center.heartInterval")
	if heartInterval <= 0 {
		heartInterval = _DEFAULT_HEART_INTERVAL
	}
	var inited bool
	for {
		if inited {
			time.Sleep(time.Duration(heartInterval) * time.Second)
		}
		inited = true
		select {
		case <-td.ctx.Done():
			return
		default:
		}
		hr, err := centerProxy.Heart(td.getCurrentTasks())
		if err != nil {
			logger.LOG_WARN("发送中心心跳请求失败，", err)
			continue
		}
		err = td.redisClient.StringSet(constants.REDIS_KEY_BOXID, hr.Node.Id)
		if err != nil {
			logger.LOG_ERROR("BoxId缓存入redis异常，", err)
		}
		td.refreshTasks(hr.Tasks)
	}
}

//获取当前正在执行的任务列表
func (td *TaskDispatcher) getCurrentTasks() (localTasks []*model.Task) {
	td.Lock()
	for _, v := range td.taskMap {
		localTasks = append(localTasks, v)
	}
	td.Unlock()
	return localTasks
}

//刷新任务列表
func (td *TaskDispatcher) refreshTasks(tasks []*model.Task) {
	if len(tasks) == 0 {
		logger.LOG_INFO("无任务更新")
		return
	}
	if len(tasks) > 0 {
		logger.LOG_WARN("任务更新，数量：", len(tasks))
		td.Lock()
		for _, t := range tasks {
			oldTask, ok := td.taskMap[t.ID]
			//非正常任务时，移除
			if t.Status != model.TASK_STATUS_RUNNING {
				delete(td.taskMap, t.ID)
				continue
			}
			//新增或变更任务
			if !ok || oldTask.UpdateTime != t.UpdateTime || oldTask.ResourceId != t.ResourceId || !bytes.Equal(oldTask.ResourceBytes, t.ResourceBytes) {
				td.taskMap[t.ID] = t
			}
		}
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

//绑定任务到执行器
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
