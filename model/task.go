package model

import (
	"bytes"
	"dyzs/galaxy/logger"
	"encoding/csv"
	"io"
)

const TASK_STATUS_NEW = 0
const TASK_STATUS_RUNNING = 1
const TASK_STATUS_STOP = 2
const TASK_STATUS_DELETED = 99

type Task struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Repository  string `json:"repository"`
	PreviousTag string `json:"previousTag"`
	CurrentTag  string `json:"currentTag"`
	AccessType  string `json:"accessType"`
	ExportPorts string `json:"exportPorts"`
	AllResource bool   `json:"allResource"`
	ResourceId  string `json:"resourceId"`
	Status      int    `json:"status"`
	AccessParam string `json:"accessParam"`
	CreateTime  int64  `json:"createTime"`
	UpdateTime  int64  `json:"updateTime"`

	NodeID        string `json:"nodeId"`
	ResourceBytes string `json:"resourceBytes"`
	resourceCache []*Resource
}

func (task *Task) GetResources() []*Resource {
	if len(task.resourceCache) > 0 {
		return task.resourceCache
	}
	if len(task.ResourceBytes) > 0 {
		csvReader := csv.NewReader(bytes.NewReader([]byte(task.ResourceBytes)))
		for {
			row, err := csvReader.Read()
			if err != nil && err != io.EOF {
				logger.LOG_WARN("资源csv解析异常，", err)
				break
			}
			if err == io.EOF {
				break
			}
			if len(row) == 0 {
				continue
			}
			task.resourceCache = append(task.resourceCache, CsvToResource(row))
		}
	}
	return task.resourceCache
}
