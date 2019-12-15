package mongo

import (
	"github.com/globalsign/mgo"
	log "github.com/sirupsen/logrus"
	"sunset/data-hub/context"
	"sunset/data-hub/logger"
)

var session *mgo.Session
var currentDB string

func Connect() (err error) {
	url := context.GetString("mongodb.url")
	db := context.GetString("mongodb.db")
	if url == "" {
		panic("mongodb 连接地址未设置")
	}
	if db == "" {
		log.Warn("mongodb 未指定库")
	}
	currentDB = db
	session, err = mgo.Dial(url)
	if err != nil {
		logger.LOG_ERROR("连接数据库异常", err)
		return
	}
	return
}

func Dataset(c string) (clt *mgo.Collection, err error) {
	if session == nil {
		err = Connect()
	}
	if session != nil {
		session.Refresh()
		return session.DB(currentDB).C(c), nil
	}
	return nil, err
}

func Close() {
	if session != nil {
		session.Close()
	}
}
