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
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Repository  string   `json:"repository"`
	PreviousTag string   `json:"previousTag"`
	CurrentTag  string   `json:"currentTag"`
	AccessType  string   `json:"accessType"`
	ManagePort  string   `json:"managePort"`
	ExportPorts []string `json:"exportPorts"`
	AllResource bool     `json:"allResource"`
	ResourceId  string   `json:"resourceId"`
	Status      int      `json:"status"`
	Config      string   `json:"config"`
	DelFlag     int      `json:"delFlag"`
	CreateTime  int64    `json:"createTime"`
	UpdateTime  int64    `json:"updateTime"`

	ResourceBytes []byte `json:"resourceBytes"`
	resourceCache []*Resource
}

func (task *Task) GetResources() []*Resource {
	if len(task.resourceCache) > 0 {
		return task.resourceCache
	}
	if len(task.ResourceBytes) > 0 {
		csvReader := csv.NewReader(bytes.NewReader(task.ResourceBytes))
		for {
			row, err := csvReader.Read()
			if err != nil && err != io.EOF {
				logger.LOG_WARN("资源csv解析异常，", err)
				break
			}
			if err == io.EOF {
				break
			}
			if len(row) < 6 {
				continue
			}
			task.resourceCache = append(task.resourceCache, &Resource{
				ID:   row[0],
				Name: row[1],
				GbID: row[2],
				Type: row[3],
			})
		}
	}
	return task.resourceCache
}
