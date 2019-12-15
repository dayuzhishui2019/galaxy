package dag_plugin_hubmsgthrift

import (
	"context"
	"dag-model/daghub"
	context2 "dag-stream/context"
	"dag-stream/logger"
	"dag-stream/operator/dag-plugin-hubmsgthrift/shared"
	"dag-stream/stream"
	"dag-stream/util"
	"errors"
	"git.apache.org/thrift.git/lib/go/thrift"
	"strconv"
	"time"
)

func init() {
	stream.RegistHandler("hubmsgthrift", &stream.HandlerWrapper{
		InitFunc:   Init,
		HandleFunc: Handle,
		CloseFunc:  Close,
	})
}

var client *shared.DAGThriftServiceClient
var transport thrift.TTransport
var address string
var retry = -1 //默认无限重试

func Init(config interface{}) error {
	address = context2.GetString("hubmsgthrift_address")
	if context2.IsExsit("hubmsgthrift_retry") {
		retry = context2.GetInt("hubmsgthrift_retry")
	}
	logger.LOG_INFO("------------------ hubmsgthrift config ------------------")
	logger.LOG_INFO("hubmsgthrift_address : " + address)
	logger.LOG_INFO("hubmsgthrift_retry : " + strconv.Itoa(retry))
	logger.LOG_INFO("------------------------------------------------------")
	if address == "" {
		return errors.New("hubmsgthrift_address缺少参数address")
	}
	go InitConnection(1)
	return nil
}

func InitConnection(retry int) {
	_ = util.Retry(func() error {
		if transport != nil {
			_ = transport.Close()
		}
		var err error
		transport, err = thrift.NewTSocket(address)
		if err != nil {
			logger.LOG_WARN("hubmsgthrift thrift 连接异常，稍后重试：", err)
			return err
		}
		transport, err = thrift.NewTBufferedTransportFactory(8192).GetTransport(transport)
		if err != nil {
			logger.LOG_WARN("hubmsgthrift thrift 连接异常，稍后重试：", err)
			return err
		}
		protocolFactory := thrift.NewTCompactProtocolFactory()
		iprot := protocolFactory.GetProtocol(transport)
		oprot := protocolFactory.GetProtocol(transport)
		client = shared.NewDAGThriftServiceClient(thrift.NewTStandardClient(iprot, oprot))
		if err := transport.Open(); err != nil {
			logger.LOG_WARN("hubmsgthrift thrift 连接异常，稍后重试：", err)
			return err
		}
		return err
	}, retry, 1*time.Second)
}

func Handle(data interface{}, next func(interface{})) {
	wraps, ok := data.([]*daghub.StandardModelWrap)
	if !ok {
		logger.LOG_INFO("")
		return
	}
	iotinfos := getIotinfo(wraps)

	util.Retry(func() error {
		_, err := client.BatchUpload(context.Background(), iotinfos)
		if err != nil {
			InitConnection(retry)
		}
		return err
	}, retry, 1*time.Second)
}

func getIotinfo(wraps []*daghub.StandardModelWrap) []*shared.IotInfo {
	iotinfos := make([]*shared.IotInfo, 0)
	for _, value := range wraps {
		byts, _ := value.ToProtoBufBytes()
		iotinfo := &shared.IotInfo{
			DataType:   value.DataType,
			RecordId:   "",
			ProxyIp:    "",
			ProxyPort:  0,
			UploadTime: 0,
			RawData:    byts,
			ResJson:    "",
			Res1Json:   "",
			Res2Json:   "",
		}
		iotinfos = append(iotinfos, iotinfo)
	}
	return iotinfos
}

func Close() error {
	if transport != nil {
		return transport.Close()
	}
	return nil
}
