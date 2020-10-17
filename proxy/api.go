package proxy

import "dyzs/galaxy/model"

type CenterProxy interface {
	//心跳、注册、获取更新任务
	Heart(localTasks []*model.Task) (*HeartResonse, error)
}

type HeartResonse struct {
	Node  model.Node    `json:"box"`
	Tasks []*model.Task `json:"tasks"`
}

func NewCenterProxy() CenterProxy {
	hcp := &HttpCenterProxy{}
	hcp.Init()
	return hcp
}
