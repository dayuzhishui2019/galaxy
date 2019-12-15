package dag_plugin_iodfacethrift

import (
	"context"
	"dag-model/constant"
	"dag-model/daghub"
	protobuf "dag-model/proto/netposa-standard"
	context2 "dag-stream/context"
	"dag-stream/logger"
	"dag-stream/operator/dag-plugin-iodfacethrift/iodface"
	"dag-stream/operator/dag-plugin-iodfacethrift/maping"
	"dag-stream/stream"
	"dag-stream/util"
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"os"
	"time"
)

func init() {
	stream.RegistHandler("iodfacethrift", &stream.HandlerWrapper{
		InitFunc:   Init,
		HandleFunc: Handle,
		CloseFunc:  Close,
	})
}

var client *iodface.ThriftProxyServiceClient
var transport thrift.TTransport

func Init(config interface{}) error {
	thriftip := context2.GetString("iodthrift_address")
	var err error
	transport, err = thrift.NewTSocket(thriftip)
	if err != nil {
		fmt.Errorf("NewTSocket failed. err: [%v]\n", err)
		return err
	}

	transport, err = thrift.NewTBufferedTransportFactory(8192).GetTransport(transport)
	if err != nil {
		fmt.Errorf("NewTransport failed. err: [%v]\n", err)
		return err
	}

	if err := transport.Open(); err != nil {
		fmt.Errorf("Transport.Open failed. err: [%v]\n", err)
		return err
	}

	protocolFactory := thrift.NewTCompactProtocolFactory()
	iprot := protocolFactory.GetProtocol(transport)
	oprot := protocolFactory.GetProtocol(transport)
	client = iodface.NewThriftProxyServiceClient(thrift.NewTStandardClient(iprot, oprot))
	if err := transport.Open(); err != nil {
		fmt.Fprintln(os.Stderr, "Error opening socket to 127.0.0.1:9898", " ", err)
	}
	return nil
}

func Handle(data interface{}, next func(interface{})) {
	wraps, ok := data.([]*daghub.StandardModelWrap)
	if !ok {
		logger.LOG_INFO("")
		return
	}
	iotinfos := getFaceInfo(wraps)
	if len(iotinfos) == 0 {
		return
	}

	util.Retry(func() error {
		res, err := client.OnUploadFaceInfos(context.Background(), iotinfos)
		logger.LOG_INFO("00000000000000000000000",res,err)
		return err
	}, -1, 1*time.Second)
	logger.LOG_INFO("send iod face success" , len(iotinfos))
}

func getFaceInfo(wraps []*daghub.StandardModelWrap) (face []*iodface.FaceInfo) {
	faceiods := make([]*iodface.FaceInfo, 0)
	for _, value := range wraps {
		if value.DataType != constant.DAG_DATATYPE_FACE {
			continue
		}
		for _, face := range value.FaceList.Facelist {
			//转换图片
			imaglis := getImageConext(face.SubImageList)
			iodid, iodip, ok := maping.Handel(face.DeviceId)
			if !ok {
				//
				continue
			}
			faceitem := &iodface.FaceInfo{
				GUID:         face.RecordId,
				ProxyIp:      "",
				ProxyPort:    0,
				UploadTime:   time.Now().Format("2006-01-02 15:04:05"),
				DevIp:        iodip,
				DevPort:      0,
				AbsTime:      time.Unix(face.AccessTime, 1000).String(),
				StayDuration: 0,
				ImageList:    imaglis,
				Res:          "",
				DevID:        iodid,
			}
			faceiods = append(faceiods, faceitem)
		}
	}
	return faceiods
}

func getImageConext(dagsubimages []*protobuf.SubImageInfo) (imagelist []*iodface.ImageContent) {
	iodimages := make([]*iodface.ImageContent, 0)
	for _, subimage := range dagsubimages {
		iodimg := &iodface.ImageContent{
			ImgType: func(imgtype int32) string {
				if imgtype == 4 || imgtype == 5 || imgtype == 11 {
					return "1"
				}
				return "0"
			}(subimage.ImageType),
			ImgUrl:  subimage.StoragePath,
			ImgData: subimage.ImageData,
		}
		iodimages = append(iodimages, iodimg)
	}
	return iodimages
}

func Close() error {
	if transport != nil {
		return transport.Close()
	}
	return nil
}
