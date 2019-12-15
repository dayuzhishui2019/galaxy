package dag_plugin_kafkaconsumer

import (
	"context"
	"dag-model/kafka"
	dagContext "dag-stream/context"
	"dag-stream/logger"
	"dag-stream/stream"
	"errors"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"strconv"
	"strings"
	"time"
)

func init() {
	stream.RegistEmitter("kafkaconsumer", &KafkaConsumer{})
}

type FromOffset int

const (
	FROM_OFFSET_NEWEST FromOffset = 0
	FROM_OFFSET_OLDEST FromOffset = 1
)

type KafkaConsumer struct {
	Bootstrap []string
	Topics    []string
	GroupId   string
	FromOffset
	BatchSize     int
	BatchDelay    int
	kafkaConsumer *cluster.Consumer
	emit          func(interface{})
	cancelCtx     context.Context
	cancel        func()
}

func (consumer *KafkaConsumer) Init(emit func(interface{})) error {
	logger.LOG_INFO("启动kafka-consumer")
	consumer.Close()
	logger.LOG_INFO("---------------- kafkaconsumer config ----------------")
	logger.LOG_INFO("kafkaconsumer_bootstrap : " + dagContext.GetString("kafkaconsumer_bootstrap"))
	logger.LOG_INFO("kafkaconsumer_topics : " + dagContext.GetString("kafkaconsumer_topics"))
	logger.LOG_INFO("kafkaconsumer_groupId : " + dagContext.GetString("kafkaconsumer_groupId"))
	logger.LOG_INFO("kafkaconsumer_fromEarliestOffset : " + strconv.FormatBool(dagContext.GetBool("kafkaconsumer_fromEarliestOffset")))
	logger.LOG_INFO("kafkaconsumer_batchSize : " + strconv.Itoa(dagContext.GetInt("kafkaconsumer_batchSize")))
	logger.LOG_INFO("kafkaconsumer_batchDelay : " + strconv.Itoa(dagContext.GetInt("kafkaconsumer_batchDelay")))
	logger.LOG_INFO("------------------------------------------------------")
	unConfigKeys := dagContext.Exsit("kafkaconsumer_bootstrap", "kafkaconsumer_topics", "kafkaconsumer_groupId")
	if len(unConfigKeys) > 0 {
		return errors.New("缺少配置：" + strings.Join(unConfigKeys, ","))
	}
	consumer.Bootstrap = strings.Split(strings.Trim(dagContext.GetString("kafkaconsumer_bootstrap"), " "), ",")
	consumer.Topics = strings.Split(strings.Trim(dagContext.GetString("kafkaconsumer_topics"), " "), ",")
	consumer.GroupId = dagContext.GetString("kafkaconsumer_groupId")
	consumer.FromOffset = FROM_OFFSET_NEWEST
	if dagContext.GetBool("kafkaconsumer_fromEarliestOffset") {
		consumer.FromOffset = FROM_OFFSET_OLDEST
	}
	consumer.BatchSize = dagContext.GetInt("kafkaconsumer_batchSize")
	consumer.BatchDelay = dagContext.GetInt("kafkaconsumer_batchDelay")
	go consumer.Start(emit)
	return nil
}

func (c *KafkaConsumer) Start(emit func(interface{})) {
	c.emit = emit
	cancelCtx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.cancelCtx = cancelCtx

	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Version = sarama.V2_1_0_0
	config.Net.DialTimeout = 5 * time.Second
	if c.FromOffset == FROM_OFFSET_NEWEST {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	} else {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

OUT_LOOP:
	for {
		select {
		case <-cancelCtx.Done():
			break OUT_LOOP
		default:
			consumer, err := cluster.NewConsumer(c.Bootstrap, c.GroupId, c.Topics, config)
			if err == nil {
				c.kafkaConsumer = consumer
				go c.handleErrors()
				go c.handleNotifications()
				c.consumeAndSend()
				break
			} else {
				logger.LOG_ERROR("创建DAG-HUB kafka consumer 失败，重新连接：", err)
			}
		}
	}
}

func (c *KafkaConsumer) handleErrors() {
OUT_LOOP:
	for {
		select {
		case err, ok := <-c.kafkaConsumer.Errors():
			if ok {
				logger.LOG_WARN("kafka消费异常", err)
			} else {
				break OUT_LOOP
			}
		case <-c.cancelCtx.Done():
			break OUT_LOOP
		}
	}
}

func (c *KafkaConsumer) handleNotifications() {
OUT_LOOP:
	for {
		select {
		case ntf, ok := <-c.kafkaConsumer.Notifications():
			if ok {
				logger.LOG_WARN(ntf, nil)
			} else {
				break OUT_LOOP
			}
		case <-c.cancelCtx.Done():
			break OUT_LOOP
		}
	}
}

func (c *KafkaConsumer) consumeAndSend() {
	if c.BatchSize == 1 {
		//single
	OUT_LOOP:
		for {
			select {
			case msg, ok := <-c.kafkaConsumer.Messages():
				if ok {
					logger.LOG_INFO("kafkaconsumer receive msg")
					c.emit([]*kafka.KafkaMessage{&kafka.KafkaMessage{
						Key:            msg.Key,
						Value:          msg.Value,
						Topic:          msg.Topic,
						Partition:      msg.Partition,
						Offset:         msg.Offset,
						Timestamp:      msg.Timestamp,
						BlockTimestamp: msg.BlockTimestamp,
					}})
					c.kafkaConsumer.MarkOffset(msg, "")
					c.kafkaConsumer.CommitOffsets()
				} else {
					logger.LOG_WARN("kafka消费失败", nil)
					break OUT_LOOP
				}
			case <-c.cancelCtx.Done():
				break OUT_LOOP
			}
		}
	} else {
		//batch
		var batchDelay time.Duration = 1 * time.Millisecond
		if c.BatchDelay > 0 {
			batchDelay = time.Duration(c.BatchDelay) * time.Millisecond
		}
		idleDelay := time.NewTimer(batchDelay)
		msgs := make([]*kafka.KafkaMessage, c.BatchSize)
		kafkaMsgs := make([]*sarama.ConsumerMessage, c.BatchSize)
	OUT_LOOP2:
		for {
			kafkaMsgs = kafkaMsgs[:0]
			msgs = msgs[:0]
			if !idleDelay.Stop() {
				select {
				case <-idleDelay.C: //try to drain from the channel
				default:
				}
			}
			idleDelay.Reset(batchDelay)
		IN_LOOP:
			for i := 0; i < c.BatchSize; i++ {
				select {
				case msg, ok := <-c.kafkaConsumer.Messages():
					if ok {
						kafkaMsgs = append(kafkaMsgs, msg)
						msgs = append(msgs, &kafka.KafkaMessage{
							Key:            msg.Key,
							Value:          msg.Value,
							Topic:          msg.Topic,
							Partition:      msg.Partition,
							Offset:         msg.Offset,
							Timestamp:      msg.Timestamp,
							BlockTimestamp: msg.BlockTimestamp,
						})
					} else {
						logger.LOG_WARN("kafka消费失败", nil)
						break OUT_LOOP2
					}
				case <-idleDelay.C:
					break IN_LOOP
				case <-c.cancelCtx.Done():
					break OUT_LOOP2
				}
			}
			if len(msgs) == 0 {
				continue
			}
			//emit
			logger.LOG_INFO("kafkaconsumer receive msgs : %d", len(msgs))
			c.emit(msgs)
			for index, _ := range msgs {
				c.kafkaConsumer.MarkOffset(kafkaMsgs[index], "")
			}
			for {
				err := c.kafkaConsumer.CommitOffsets()
				if err == nil {
					break
				}
				logger.LOG_WARN("提交kafka offset异常，重试", err)
				time.Sleep(1 * time.Second)
			}
		}
	}
}

func (c *KafkaConsumer) Close() error {
	if c.kafkaConsumer != nil {
		err := c.kafkaConsumer.Close()
		if err != nil {
			logger.LOG_WARN("关闭kafka消费者异常", err)
			return err
		}
	}
	if c.cancel != nil {
		c.cancel()
	}
	return nil
}
