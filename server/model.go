package server

import jsoniter "github.com/json-iterator/go"

type GalaxyCmd struct {
	Cmd   string              `json:"cmd"`
	Param jsoniter.RawMessage `json:"param"`
}

type GalaxyResponse struct{
	Code int `json:"code"`
	Message string `json:"message"`
	Result interface{} `json:"result"`
}

type Channel struct{
	ResourceId string
	ChannelNo string
	Name string
}