package model

type Group struct {
	ID       string  `json:"id" bson:"id"`
	GbID     string  `json:"gbId" bson:"gbId" binding:"required"`
	Name     string  `json:"name" bson:"name" binding:"required"`
	Type     string  `json:"type" bson:"type"`
	ParentID float32 `json:"parentId" bson:"parentId" binding:"required"`
}
