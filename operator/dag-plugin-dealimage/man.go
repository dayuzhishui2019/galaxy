package dag_plugin_dealimage

import (
	"dag-model/daghub"
	protobuf "dag-model/proto/netposa-standard"
	"dag-stream/concurrent"
	"dag-stream/context"
	"dag-stream/logger"
	"dag-stream/stream"
	"dag-stream/util"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"
)

func init() {
	stream.RegistHandler("imagedeal", &stream.HandlerWrapper{
		InitFunc:   Init,
		HandleFunc: Handle,
		CloseFunc:  Close,
	})
}

var executor *concurrent.Executor

func Init(config interface{}) error {
	capacity := 20
	configCapacity := context.GetInt("imagedeal_capacity")
	if configCapacity > 0 {
		capacity = configCapacity
	}
	logger.LOG_INFO("------------------ imagedeal config ------------------")
	logger.LOG_INFO("imagedeal_capacity : " + strconv.Itoa(capacity))
	logger.LOG_INFO("------------------------------------------------------")
	e, err := concurrent.NewExecutor(capacity)
	executor = e
	if err != nil {
		return err
	}
	return nil
}

func Handle(data interface{}, next func(interface{})) {
	wraps, ok := data.([]*daghub.StandardModelWrap)
	if !ok {
		logger.LOG_ERROR("imagedeal 转换数据异常", nil)
		return
	}
	if len(wraps) == 0 {
		return
	}
	tasks := make([]func(), 0)
	for _, wrap := range wraps {
		for _, item := range wrap.GetSubImageInfos() {
			func(img *protobuf.SubImageInfo) {
				tasks = append(tasks, func() {
					downloadImage(img)
				})
			}(item)
		}
	}
	err := executor.SubmitSyncBatch(tasks)
	if err != nil {
		logger.LOG_ERROR("批量下载图片失败：", err)
	}
	next(wraps)
}

func downloadImage(image *protobuf.SubImageInfo) {
	url := image.StoragePath
	if url == "" {
		logger.LOG_WARN("图片路径缺失：", nil)
		return
	}
	if len(image.ImageData) > 0 {
		return
	}
	err := util.Retry(func() error {
		res, err := http.Get(url)
		if err != nil {
			return err
		}
		defer func() {
			err := res.Body.Close()
			if err != nil {
				logger.LOG_WARN("下载图片,关闭res失败：url - "+url, err)
			}
		}()
		bytes, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return err
		}
		image.ImageData = bytes
		return nil
	}, 3, 100*time.Millisecond)

	if err != nil {
		logger.LOG_WARN("下载图片失败：url - "+url, err)
	}
}

func Close() error {
	if executor != nil {
		executor.Close()
	}
	return nil
}
