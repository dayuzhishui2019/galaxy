namespace go shared   


struct IotInfo {
1: string dataType
2: string recordId 
3: string proxyIp 
4: i32 proxyPort
5: i64 uploadTime
6: binary rawData
7: string resJson
8: string res1Json
9: string res2Json
}


service DAGThriftService
{   
	i32 upload(1:IotInfo data) 
	i32 batchUpload(1:list<IotInfo> datas) 

}