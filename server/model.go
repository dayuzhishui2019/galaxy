package server

import jsoniter "github.com/json-iterator/go"

type GalaxyCmd struct {
	Cmd   string              `json:"cmd"`
	Param jsoniter.RawMessage `json:"param"`
}

type GalaxyResponse struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Result  interface{} `json:"result"`
}

type Channel struct {
	DeviceID            string  `json:"DeviceID"` // 该字段为通道ID
	Name                string  `json:"Name"`
	Manufacturer        string  `json:"Manufacturer"`
	Model               string  `json:"Model"`
	Owner               string  `json:"Owner"`
	CivilCode           string  `json:"CivilCode"`
	Block               string  `json:"Block"`
	Address             string  `json:"Address"`
	Parental            int     `json:"Parental"`
	ParentID            string  `json:"ParentID"`
	SafetyWay           int     `json:"SafetyWay"`
	RegisterWay         string  `json:"RegisterWay"`
	CertNum             string  `json:"CertNum"`
	Certifiable         int     `json:"Certifiable"`
	ErrCode             int     `json:"ErrCode"`
	EndTime             string  `json:"EndTime"`
	Secrecy             int     `json:"Secrecy"`
	IPAddress           string  `json:"IPAddress"`
	Port                int     `json:"Port"`
	Password            string  `json:"Password"`
	Status              string  `json:"Status"`
	Longitude           float64 `json:"Longitude"`
	Latitude            float64 `json:"Latitude"`
	PTZType             int     `json:"PTZType"`
	PositionType        int     `json:"PositionType"`
	UseType             int     `json:"UseType"`
	SupplyLightType     int     `json:"SupplyLightType"`
	DirectionType       int     `json:"DirectionType"`
	Resolution          int     `json:"Resolution"`
	BusinessGroupID     string  `json:"BusinessGroupID"`
	DownloadSpeed       int     `json:"DownloadSpeed"`
	SVCSpaceSupportMode int     `json:"SVCSpaceSupportMode"`
	SVCTimeSupportMode  int     `json:"SVCTimeSupportMode"`
	FromID              string  `json:"FromID"` //设备ID
}

type CatalogListParam struct{
	CatalogList []*Channel `json:"CatalogList"`
}