package model

const TASK_DELETED_FLAG = 1

type Task struct {
	ID          string   `json:"id" bson:"id"`
	Name        string   `json:"name" bson:"name" binding:"required"`
	TaskType    string   `json:"taskType" bson:"taskType" binding:"required"`
	ManagePort  string   `json:"managePort" bson:"managePort" binding:"required"`
	ExportPorts []string `json:"exportPorts" bson:"exportPorts"`
	AllResource bool     `json:"allResource" bson:"allResource"`
	ResourceIds []string `json:"resourceIds" bson:"resourceIds"`
	Status      int      `json:"status" bson:"status"`
	Config      string   `json:"config" bson:"config"`
	DelFlag     int      `json:"delFlag" bson:"delFlag"`
	CreateTime  int64    `json:"createTime" bson:"createTime"`
	UpdateTime  int64    `json:"updateTime" bson:"updateTime"`
}
