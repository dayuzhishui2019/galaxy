package dag_plugin_kafkaproducer

import (
	"dag-model/kafka"
	"dag-stream/context"
	"dag-stream/logger"
	"dag-stream/stream"
	"dag-stream/util"
	"errors"
	"github.com/Shopify/sarama"
	"strings"
	"time"
)

func init() {
	stream.RegistHandler("kafkaproducer", &KafkaProducer{})
}

type KafkaProducer struct {
	Bootstrap     []string
	kafkaProducer sarama.SyncProducer
}

func (p *KafkaProducer) Init(config interface{}) error {
	logger.LOG_INFO("启动 kafka-producer")
	logger.LOG_INFO("---------------- kafkaproducer config ----------------")
	logger.LOG_INFO("kafkaproducer_bootstrap : " + context.GetString("kafkaproducer_bootstrap"))
	logger.LOG_INFO("------------------------------------------------------")
	unConfigKeys := context.Exsit("kafkaproducer_bootstrap")
	if len(unConfigKeys) > 0 {
		return errors.New("缺少配置：" + strings.Join(unConfigKeys, ","))
	}
	p.Bootstrap = strings.Split(strings.Trim(context.GetString("kafkaproducer_bootstrap"), " "), ",")
	go p.InitConnection(1)
	return nil
}

func (p *KafkaProducer) InitConnection(retry int) {
	_ = util.Retry(func() error {
		_ = p.Close()
		syncProducer, err := sarama.NewSyncProducer(p.Bootstrap, nil)
		p.kafkaProducer = syncProducer
		if err != nil {
			logger.LOG_ERROR("创建同步kafka-producer失败", err)
			return err
		}
		return nil
	}, retry, 1*time.Second)
}

func (p *KafkaProducer) Handle(data interface{}, next func(interface{})) {
	msgs, ok := data.([]*kafka.KafkaMessage)
	if !ok {
		logger.LOG_ERROR("数据格式转换为kafka-message失败", nil)
		return
	}
	if len(msgs) == 0 {
		return
	}
	kafkamsgs := Cast(msgs)
	_ = util.Retry(func() error {
		err := p.kafkaProducer.SendMessages(kafkamsgs)
		if err != nil {
			//发送异常、重连
			p.InitConnection(-1)
		}
		return err
	}, -1, 1*time.Second)
	logger.LOG_INFO("kafkaproducer send msgs：%d", len(kafkamsgs))
}

func Cast(msgs []*kafka.KafkaMessage) []*sarama.ProducerMessage {
	kafkaMsgs := make([]*sarama.ProducerMessage, 0)
	for _, msg := range msgs {
		bytes := sarama.ByteEncoder(msg.Value)
		kafkaMsgs = append(kafkaMsgs, &sarama.ProducerMessage{
			Topic: msg.Topic,
			Key:   nil,
			Value: bytes,
			//Headers:   nil,
			//Metadata:  nil,
			//Offset:    0,
			//Partition: 0,
			//Timestamp: time.Time{},
		})
		logger.LOG_DEBUG("单条消息大小：%d", len(bytes))
	}
	return kafkaMsgs
}

func (p *KafkaProducer) Close() error {
	if p.kafkaProducer != nil {
		err := p.kafkaProducer.Close()
		if err != nil {
			logger.LOG_WARN("关闭kafka生产者异常", err)
		}
	}
	return nil
}
