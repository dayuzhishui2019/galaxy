package m

import (
	"dag-model/constant"
	"dag-model/daghub"
	"dag-model/kafka"
	pbhub "dag-model/proto/daghub"
	pbnp "dag-model/proto/netposa-standard"
	"dag-stream/context"
	"dag-stream/logger"
	"github.com/golang/protobuf/proto"
	"strconv"
	"time"
)

var reBatchSize = 0

const TOPIC_KAFKA_MESSAGE_DATA = "KAFKA_MESSAGE_DATA"

var TOPIC_DATATYPE_MAPPING = map[string]string{}

func Init(config interface{}) error {
	rbs := context.GetInt("bytetohubmsg_reBatchSize")
	if rbs > 0 {
		reBatchSize = rbs
	}
	logger.LOG_INFO("---------------- bytetohubmsg config -----------------")
	logger.LOG_INFO("bytetohubmsg_reBatchSize : " + strconv.Itoa(rbs))
	logger.LOG_INFO("------------------------------------------------------")
	//topic mapping
	TOPIC_DATATYPE_MAPPING["dag-row"] = TOPIC_KAFKA_MESSAGE_DATA
	TOPIC_DATATYPE_MAPPING["dag-wifi"] = TOPIC_KAFKA_MESSAGE_DATA
	TOPIC_DATATYPE_MAPPING["dag-gps"] = TOPIC_KAFKA_MESSAGE_DATA
	TOPIC_DATATYPE_MAPPING["dag-rfid"] = TOPIC_KAFKA_MESSAGE_DATA
	return nil
}

/**
将kafka消息转换为摘要对象,根据数据类型、资源等信息过滤
*/
func Handle(data interface{}, next func(interface{})) {
	kafkaMsgs, ok := data.([]*kafka.KafkaMessage)
	if !ok {
		logger.LOG_ERROR("bytetohubmsg:数据转换kafka.KafkaMessage失败", nil)
		return
	}
	if len(kafkaMsgs) == 0 {
		return
	}
	wraps := make([]*daghub.StandardModelWrap, 0)

	for _, kafkaMsg := range kafkaMsgs {
		var dataBytes []byte
		var dataType string
		topicDataType := TOPIC_DATATYPE_MAPPING[kafkaMsg.Topic]
		if topicDataType == TOPIC_KAFKA_MESSAGE_DATA {
			msgData := &pbhub.KafkaMessageData{}
			err := proto.Unmarshal(kafkaMsg.Value, msgData)
			if err != nil {
				logger.LOG_ERROR("dag-hub 消息转化失败", err)
				continue
			}
			dataType = msgData.Datatype
			dataBytes = msgData.Data
		} else if topicDataType != "" {
			dataType = topicDataType
			dataBytes = kafkaMsg.Value
		} else {
			logger.LOG_WARN("dag-hub 消息未知的topic:"+kafkaMsg.Topic, nil)
			continue
		}
		//根据数据类型过滤
		if filterByDataType(dataType) {
			logger.LOG_INFO("过滤数据", nil)
			continue
		}
		//转换企标
		wrap, err := daghub.FromProtoBufBytes(dataType, dataBytes)
		if err != nil {
			logger.LOG_ERROR("DagHub 数据转换 企标数据失败["+dataType+"]:", err)
			continue
		}
		wraps = append(wraps, wrap)
	}
	if len(wraps) <= 0 {
		return
	}
	if reBatchSize > 0 {
		next(reBatchWraps(wraps))
	} else {
		next(wraps)
	}
}

func filterByDataType(dataType string) bool {
	return false
}

func filterByResource(id string) bool {
	return false
	if context.ExsitResource(id) {
		return false
	}
	logger.LOG_INFO("filtered by resourceId : %s", id)
	return true
}

func reBatchWraps(rawWraps []*daghub.StandardModelWrap) (newWraps []*daghub.StandardModelWrap) {
	faceSli := make([]*pbnp.Face, 0)
	personSli := make([]*pbnp.Person, 0)
	vehicleSli := make([]*pbnp.MotorVehicle, 0)
	nonVehicleSli := make([]*pbnp.NonMotorVehicle, 0)

	newWraps = make([]*daghub.StandardModelWrap, 0)
	//flat and filter
	for _, wrap := range rawWraps {
		if wrap == nil {
			continue
		}
		switch wrap.DataType {
		case constant.DAG_DATATYPE_FACE:
			for _, item := range wrap.FaceList.Facelist {
				if !filterByResource(item.DeviceId) {
					faceSli = append(faceSli, item)
				}
			}
		case constant.DAG_DATATYPE_BODY:
			for _, item := range wrap.PersonList.Personlist {
				if !filterByResource(item.DeviceId) {
					personSli = append(personSli, item)
				}
			}
		case constant.DAG_DATATYPE_VEHICLE:
			for _, item := range wrap.MotorVehicleList.Vehiclelist {
				if !filterByResource(item.DeviceId) {
					vehicleSli = append(vehicleSli, item)
				}
			}
		case constant.DAG_DATATYPE_NONMOTOR:
			for _, item := range wrap.NonMotorVehicleList.NonVehiclelist {
				if !filterByResource(item.DeviceId) {
					nonVehicleSli = append(nonVehicleSli, item)
				}
			}
		}
	}

	//batch
	partitions(len(faceSli), reBatchSize, func(start, end int) {
		newWraps = append(newWraps, &daghub.StandardModelWrap{
			DataType: constant.DAG_DATATYPE_FACE,
			FaceList: &pbnp.FaceList{
				Facelist: faceSli[start:end],
			},
		})
	})
	partitions(len(personSli), reBatchSize, func(start, end int) {
		newWraps = append(newWraps, &daghub.StandardModelWrap{
			DataType: constant.DAG_DATATYPE_BODY,
			PersonList: &pbnp.PersonList{
				Personlist: personSli[start:end],
			},
		})
	})
	partitions(len(vehicleSli), reBatchSize, func(start, end int) {
		newWraps = append(newWraps, &daghub.StandardModelWrap{
			DataType: constant.DAG_DATATYPE_VEHICLE,
			MotorVehicleList: &pbnp.MotorVehicleList{
				Vehiclelist: vehicleSli[start:end],
			},
		})
	})
	partitions(len(nonVehicleSli), reBatchSize, func(start, end int) {
		newWraps = append(newWraps, &daghub.StandardModelWrap{
			DataType: constant.DAG_DATATYPE_NONMOTOR,
			NonMotorVehicleList: &pbnp.NonMotorVehicleList{
				NonVehiclelist: nonVehicleSli[start:end],
			},
		})
	})
	count += len(faceSli) + len(personSli) + len(vehicleSli) + len(nonVehicleSli)
	if !startB {
		startB = true
		start = time.Now()
	}
	logger.LOG_INFO("发送数据量：%d，耗时 %v", count, time.Since(start))
	return newWraps
}

var count = 0
var start time.Time
var startB bool

func partitions(count int, batchSize int, cb func(start, end int)) {
	start := 0
	for start < count {
		end := start + batchSize
		if end > count {
			end = count
		}
		cb(start, end)
		start += batchSize
	}
}
