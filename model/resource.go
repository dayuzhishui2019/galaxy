package model

import (
	"reflect"
	"strconv"
)

type Resource struct {
	ID           string `json:"id" csv:"0"`
	GbID         string `json:"gbId" csv:"1"`
	ParentId     string `json:"parentId" csv:"2"`
	AreaNumber   string `json:"areaNumber" csv:"3"`
	DominionCode string `json:"dominionCode" csv:"4"`
	Type         string `json:"type" csv:"5"`
	Func         string `json:"func" csv:"6"`
	MvcIP        string `json:"mvcIP" csv:"7"`
	MvcPort      string `json:"mvcPort" csv:"8"`
	MvcUsername  string `json:"mvcUsername" csv:"9"`
	MvcPassword  string `json:"mvcPassword" csv:"10"`
	MvcChannels  string `json:"mvcChannels" csv:"11"`

	Name string `json:"name"`
}

var resourceTagCache map[int]string

func CsvToResource(values []string) *Resource {
	if resourceTagCache == nil {
		resourceTagCache = make(map[int]string)
		r := Resource{}
		t := reflect.TypeOf(r)
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			//Get the field tag value
			tag := field.Tag.Get("csv")
			index, err := strconv.Atoi(tag)
			if err != nil {
				continue
			}
			if len(values) < index {
				continue
			}
			resourceTagCache[index] = field.Name
		}
	}
	r := &Resource{}
	rv := reflect.ValueOf(r)
	re := rv.Elem()
	for i, v := range values {
		f, ok := resourceTagCache[i]
		if !ok {
			continue
		}
		rf := re.FieldByName(f)
		rf.SetString(v)
	}
	return r
}
