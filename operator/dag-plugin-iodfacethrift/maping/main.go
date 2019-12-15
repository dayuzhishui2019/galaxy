package maping

import (
	"encoding/json"
	"io/ioutil"
	"strings"
)
var mapping = make(map[string]string)

func init(){
	//初始化加载映射关系
	load("./mapping.json", &mapping)
}

func Handel(value string) (string,string,bool) {
	v,ok:=mapping[value]
	if !ok{
		return "","",ok
	}
	str := strings.Trim(v," ")
	items := strings.Split(str,"@")
	return items[0],items[1],true
}

func load(filename string, v interface{}) {
	//ReadFile函数会读取文件的全部内容，并将结果以[]byte类型返回
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return
	}
	//读取的数据为json格式，需要进行解码
	err = json.Unmarshal(data, v)
	if err != nil {
		return
	}
}

func main() {
	//inits()
	//id,ip := Handel("32fde740-123e-4e68-ae98-8e4543413b9f")
	//fmt.Print("####:"+id+","+ip+"&&&")
}


