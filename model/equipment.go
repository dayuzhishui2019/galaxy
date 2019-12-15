package model

type Resource struct {
	ID   string  `json:"id" bson:"id"`
	GbID string  `json:"gbId" bson:"gbId" binding:"required"`
	Name string  `json:"name" bson:"name" binding:"required"`
	Type string  `json:"type" bson:"type" binding:"required"`
	Lon  float32 `json:"lon" bson:"lon"`
	Lat  float32 `json:"lat" bson:"lat"`
	UpdateTime int64 `json:"updateTime"`
}
