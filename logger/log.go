package logger

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"os"
	"path"
	"strings"
	"time"
)

var _logLevelMap = map[string]log.Level{
	"panic": log.PanicLevel,
	"fatal": log.FatalLevel,
	"error": log.ErrorLevel,
	"warn":  log.WarnLevel,
	"info":  log.InfoLevel,
	"debug": log.DebugLevel,
	"trace": log.TraceLevel,
}

func Init() {

	level := log.InfoLevel
	configLogLevel := viper.GetString("log.level")
	if configLogLevel != "" {
		l, ok := _logLevelMap[strings.ToLower(configLogLevel)]
		if ok {
			level = l
		}
	}

	log.SetLevel(level)

	var currentLogFile *os.File
	var currentLogFileName string
	var err error

	dir := path.Join(GetAppPath(), "logs")
	exist, err := PathExists(dir)
	if err == nil {
		if !exist {
			// 创建文件夹
			err := os.Mkdir(dir, os.ModePerm)
			if err != nil {
				fmt.Printf("mkdir failed![%v]\n", err)
			} else {
				fmt.Printf("mkdir success!\n")
			}
		}
	} else {
		fmt.Println(err)
	}

	go func() {
		for {
			logfileName := genLogFileName(time.Now())
			if currentLogFile == nil || currentLogFileName != logfileName {
				currentLogFileName = logfileName
				if currentLogFile != nil {
					currentLogFile.Close()
				}
				currentLogFile, err = os.OpenFile(currentLogFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
				if err != nil {
					log.Error("Fail to create log file :", err)
					return
				}
				log.SetOutput(currentLogFile)
				removeLogFile(genLogFileName(time.Now().Add(time.Duration(-72) * time.Hour)))
			}
			time.Sleep(time.Duration(1) * time.Minute)
		}
	}()

}

func genLogFileName(date time.Time) string {
	return path.Join(GetAppPath(), "logs", "data-hub."+date.Format("20060102")+".log")
}

func GetAppPath() string {
	return os.Args[0][:(strings.LastIndex(os.Args[0], string(os.PathSeparator)) + 1)]
}

func removeLogFile(logName string) {
	err := os.Remove(logName)
	if err != nil {
		log.Warn("Fail to remove log file:", err)
	} else {
		log.Info("Success to remove log file:", logName)
	}
}

// 判断文件夹是否存在
func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func LOG_DEBUG(vars ...interface{}) {
	if log.GetLevel()==log.DebugLevel{
		fmt.Println(vars...)
	}
	log.Debug(vars...)
}

func LOG_TRACE(vars ...interface{}) {
	if log.GetLevel()==log.DebugLevel{
		fmt.Println(vars...)
	}
	log.Trace(vars...)
}

func LOG_INFO(vars ...interface{}) {
	if log.GetLevel()==log.DebugLevel{
		fmt.Println(vars...)
	}
	log.Info(vars...)
}

func LOG_WARN(vars ...interface{}) {
	if log.GetLevel()==log.DebugLevel{
		fmt.Println(vars...)
	}
	log.Warn(vars...)
}

func LOG_ERROR(vars ...interface{}) {
	if log.GetLevel()==log.DebugLevel{
		fmt.Println(vars...)
	}
	log.Error(vars...)
}
