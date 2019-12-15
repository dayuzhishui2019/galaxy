package dag_plugin_hubmsgbyte

import (
	"dag-model/constant"
	"dag-model/daghub"
	"dag-model/kafka"
	"dag-stream/logger"
	"dag-stream/stream"
)

//企标转byte

var TOPIC_MAPPING map[string]string

func init() {

	stream.RegistHandler("hubmsgtobyte", &stream.HandlerWrapper{
		InitFunc:   Init,
		HandleFunc: Handle,
	})
}

func Init(config interface{}) error {
	TOPIC_MAPPING = make(map[string]string)

	TOPIC_MAPPING[constant.DAG_DATATYPE_FACE] = "go-test-face"
	TOPIC_MAPPING[constant.DAG_DATATYPE_BODY] = "go-test-body"
	TOPIC_MAPPING[constant.DAG_DATATYPE_VEHICLE] = "go-test-vehicle"
	TOPIC_MAPPING[constant.DAG_DATATYPE_NONMOTOR] = "go-test-nonmotor"

	return nil
}

func Handle(data interface{}, next func(interface{})) {
	standardModels := data.([]*daghub.StandardModelWrap)
	kafkaMsgs := make([]*kafka.KafkaMessage, 0)
	for _, standardModel := range standardModels {
		topic, ok := TOPIC_MAPPING[standardModel.DataType]
		if !ok {
			logger.LOG_WARN("未定义数据类型的目标topic： "+standardModel.DataType, nil)
			continue
		}
		bytes, err := standardModel.ToProtoBufBytes()
		if err != nil {
			logger.LOG_WARN("hubmsgtobyte 转换失败", err)
		}
		kafkaMsgs = append(kafkaMsgs, &kafka.KafkaMessage{
			Topic: topic,
			Value: bytes,
		})
	}
	if len(kafkaMsgs) > 0 {
		next(kafkaMsgs)
	}
}
