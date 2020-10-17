package server

import jsoniter "github.com/json-iterator/go"

type GalaxyCmd struct {
	Cmd   string              `json:"cmd"`
	Param jsoniter.RawMessage `json:"param"`
}

type GalaxyResponse struct {
	Code    int         `json:"Code"`
	Message string      `json:"Message"`
	Result  interface{} `json:"Result"`
}

type Channel struct {
	FromID       string `json:"FromId"`   // 该字段标识来源设备ID，即下级国标编号
	DeviceID     string `json:"DeviceId"` // 该字段为通道ID
	Name         string `json:"Name"`
	Manufacturer string `json:"Manufacturer"`
	Model        string `json:"Model"`
	Owner        string `json:"Owner"`
	CivilCode    string `json:"CivilCode"`
	Address      string `json:"Address"`
	Parental     int    `json:"Parental"`
	ParentID     string `json:"ParentId"`
	SafetyWay    int    `json:"SafetyWay"`
	RegisterWay  int    `json:"RegisterWay"`
	Secrecy      int    `json:"Secrecy"`

	Status    string  `json:"Status"`
	Longitude float64 `json:"Longitude"`
	Latitude  float64 `json:"Latitude"`
	PTZType   int     `json:"PTZType"`
}

type CatalogListParam struct {
	CatalogList []*Channel `json:"CatalogList"`
}
