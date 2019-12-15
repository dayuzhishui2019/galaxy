package dag_plugin_bytetohubmsg

import (
	"dag-model/constant"
	"dag-model/daghub"
	"dag-model/kafka"
	pbhub "dag-model/proto/daghub"
	pbnp "dag-model/proto/netposa-standard"
	"dag-stream/context"
	"dag-stream/logger"
	"dag-stream/operator/dag-plugin-bytetohubmsg/m"
	"dag-stream/stream"
	"fmt"
	"github.com/golang/protobuf/proto"
)

func init() {
	stream.RegistHandler("bytetohubmsg", &stream.HandlerWrapper{
		InitFunc:   m.Init,
		HandleFunc: m.Handle,
	})
}

var faceSli = make([]*pbnp.Face, 0)
var personSli = make([]*pbnp.Person, 0)
var vehicleSli = make([]*pbnp.MotorVehicle, 0)
var nonVehicleSli = make([]*pbnp.NonMotorVehicle, 0)

const batchSize = 50

/**
将kafka消息转换为摘要对象,根据数据类型、资源等信息过滤
*/
func Handle(data interface{}, next func(interface{})) {
	//hubMsg := data.([]byte)
	hubMsg := data.([]*kafka.KafkaMessage)
	wraps := make([]*daghub.StandardModelWrap, 0)
	for _, kafksMsg := range hubMsg {
		model := &pbhub.KafkaMessageData{}
		//model := &kafka.KafkaMessage{}
		err := proto.Unmarshal(kafksMsg.Value, model)
		if err != nil {
			logger.LOG_ERROR("dag-hub 消息转化失败", err)
			continue
		}
		/*model := &pbhub.KafkaMessageData{}
		err := proto.Unmarshal(hubMsg, model)*/

		//根据数据类型过滤
		if filter(model) {
			logger.LOG_WARN("过滤数据", nil)
			continue
		}
		//转换企标
		sm, err := castProtoToStruct(model)
		if err != nil || sm == nil {
			logger.LOG_WARN("数据格式转换企标失败", err)
			continue
		}

		wraps = append(wraps, sm)
	}
	if len(wraps) <= 0 {
		return
	}
	standardModels := BatchWraps(wraps)
	next(standardModels)
	//清空
	clearSli()

}

func BatchWraps(wraps []*daghub.StandardModelWrap) (models []*daghub.StandardModelWrap) {

	models = make([]*daghub.StandardModelWrap, 0)
	for _, wrap := range wraps {
		if wrap == nil {
			continue
		}
		switch wrap.DataType {
		case constant.DAG_DATATYPE_FACE:
			FaceBatch(wrap, models)
		case constant.DAG_DATATYPE_BODY:
			PersonBatch(wrap, models)
		case constant.DAG_DATATYPE_VEHICLE:
			VehicleBatch(wrap, models)
		case constant.DAG_DATATYPE_NONMOTOR:
			NonVehicleBatch(wrap, models)
		}
	}
	return
}

func FaceBatch(wrap *daghub.StandardModelWrap, models []*daghub.StandardModelWrap) {
	for len(faceSli) > 0 {
		i := batchSize
		if len(faceSli) < batchSize {
			i = len(faceSli)
		}
		wrap.FaceList.Facelist = faceSli[:i]
		models = append(models, wrap)
		faceSli = faceSli[i:]

	}
}

func PersonBatch(wrap *daghub.StandardModelWrap, models []*daghub.StandardModelWrap) {
	for len(personSli) > 0 {
		i := batchSize
		if len(personSli) < batchSize {
			i = len(personSli)
		}
		wrap.PersonList.Personlist = personSli[:i]
		models = append(models, wrap)
		personSli = personSli[i:]
	}
}

func VehicleBatch(wrap *daghub.StandardModelWrap, models []*daghub.StandardModelWrap) {
	for len(vehicleSli) > 0 {
		i := batchSize
		if len(vehicleSli) < batchSize {
			i = len(vehicleSli)
		}
		wrap.MotorVehicleList.Vehiclelist = vehicleSli[:i]
		models = append(models, wrap)
		vehicleSli = vehicleSli[i:]
	}
}

func NonVehicleBatch(wrap *daghub.StandardModelWrap, models []*daghub.StandardModelWrap) {
	for len(nonVehicleSli) > 0 {
		i := batchSize
		if len(nonVehicleSli) < batchSize {
			i = len(faceSli)
		}
		wrap.NonMotorVehicleList.NonVehiclelist = nonVehicleSli[:i]
		models = append(models, wrap)
		nonVehicleSli = nonVehicleSli[i:]

	}
}

/**
TODO 根据数据过滤
*/
func filter(data *pbhub.KafkaMessageData) bool {
	fmt.Println("DataType:", data.Datatype)
	return false
}

/**
TODO 转换格式
*/
func castProtoToStruct(data *pbhub.KafkaMessageData) (model *daghub.StandardModelWrap, err error) {
	model = &daghub.StandardModelWrap{}
	model.DataType = data.Datatype
	switch data.Datatype {
	case constant.DAG_DATATYPE_FACE:
		faceList := &pbnp.FaceList{}
		err = proto.Unmarshal(data.Data, faceList)
		model.FaceList = faceList
		newFaceList := filterByFaceDeviceId(faceList) //过滤
		if newFaceList == nil {
			//清空faceSli
			faceSli = faceSli[0:0]
			return nil, err
		}
		model.FaceList = newFaceList
		//bytes, _ := json.Marshal(faceList)
		//fmt.Printf("faceList: %s\n", util.BytesString(bytes))
	case constant.DAG_DATATYPE_BODY:
		personList := &pbnp.PersonList{}
		err = proto.Unmarshal(data.Data, personList)
		newPersonList := filterByPersonDeviceId(personList)
		if newPersonList == nil {
			//清空personSli
			personSli = personSli[0:0]
			return nil, err
		}
		model.PersonList = newPersonList
	case constant.DAG_DATATYPE_VEHICLE:
		vehicleList := &pbnp.MotorVehicleList{}
		err = proto.Unmarshal(data.Data, vehicleList)
		newVehicleList := filterByVehicleDeviceId(vehicleList)
		if newVehicleList == nil {
			vehicleSli = vehicleSli[0:0]
			return nil, err
		}
		model.MotorVehicleList = newVehicleList
	case constant.DAG_DATATYPE_NONMOTOR:
		nonVehicleList := &pbnp.NonMotorVehicleList{}
		err = proto.Unmarshal(data.Data, nonVehicleList)
		newNonVehicleList := filterByNonVehicleDeviceId(nonVehicleList)
		if newNonVehicleList == nil {
			nonVehicleSli = nonVehicleSli[0:0]
			return nil, err
		}
		model.NonMotorVehicleList = newNonVehicleList
	}
	if err != nil {
		logger.LOG_ERROR("DagHub 数据转换 企标数据失败", err)
	}
	return
}

//根据face deviceId过滤
func filterByFaceDeviceId(faceList *pbnp.FaceList) (newFaceList *pbnp.FaceList) {
	newFaceList = &pbnp.FaceList{}
	newFaces := make([]*pbnp.Face, 0)
	for _, face := range faceList.Facelist {
		if !IsContains(face.DeviceId) {
			fmt.Println("=======过滤deviceId: ", face.DeviceId)
			continue
		}
		//包含
		newFaces = append(newFaces, face)
		faceSli = append(faceSli, face)
	}
	if len(newFaces) == 0 {
		newFaceList.Facelist = nil
	} else {
		newFaceList.Facelist = newFaces
	}
	return
}

//根据person deviceId过滤
func filterByPersonDeviceId(personList *pbnp.PersonList) (newPersonList *pbnp.PersonList) {
	newPersonList = &pbnp.PersonList{}
	newPersons := make([]*pbnp.Person, 0)
	for _, person := range personList.Personlist {
		if !IsContains(person.DeviceId) {
			fmt.Println("=======过滤deviceId: ", person.DeviceId)
			continue
		}
		newPersons = append(newPersons, person)
		personSli = append(personSli, person)
	}
	if len(newPersons) == 0 {
		newPersonList = nil
	} else {
		newPersonList.Personlist = newPersons
	}
	return
}

//根据vehicle deviceId过滤
func filterByVehicleDeviceId(vehicleList *pbnp.MotorVehicleList) (newVehicleList *pbnp.MotorVehicleList) {
	newVehicleList = &pbnp.MotorVehicleList{}
	newVehicles := make([]*pbnp.MotorVehicle, 0)
	for _, vehicle := range vehicleList.Vehiclelist {
		if !IsContains(vehicle.DeviceId) {
			fmt.Println("=======过滤deviceId: ", vehicle.DeviceId)
			continue
		}
		newVehicles = append(newVehicles, vehicle)
	}
	if len(newVehicles) == 0 {
		newVehicleList.Vehiclelist = nil
	} else {
		newVehicleList.Vehiclelist = newVehicles
	}
	return
}

//根据nonVehicle deviceId过滤
func filterByNonVehicleDeviceId(nonVehicleList *pbnp.NonMotorVehicleList) (newNonVehicleList *pbnp.NonMotorVehicleList) {
	newNonVehicleList = &pbnp.NonMotorVehicleList{}
	newNonVehicles := make([]*pbnp.NonMotorVehicle, 0)
	for _, nonVehicle := range nonVehicleList.NonVehiclelist {
		if !IsContains(nonVehicle.DeviceId) {
			fmt.Println("=======过滤deviceId: ", nonVehicle.DeviceId)
			continue
		}
		newNonVehicles = append(newNonVehicles, nonVehicle)
	}
	if len(newNonVehicles) == 0 {
		newNonVehicleList.NonVehiclelist = nil
	} else {
		newNonVehicleList.NonVehiclelist = newNonVehicles
	}
	return
}

//判断元素是否在slice中
func IsContains(element string) bool {
	return context.ExsitGbId(element) || context.ExsitResourceId(element)
}

func clearSli() {
	faceSli = faceSli[0:0]
	personSli = personSli[0:0]
	vehicleSli = vehicleSli[0:0]
	nonVehicleSli = nonVehicleSli[0:0]
}
