package operator

import (
	_ "dag-stream/operator/dag-plugin-bytetohubmsg"
	_ "dag-stream/operator/dag-plugin-dealimage"
	_ "dag-stream/operator/dag-plugin-hubmsgbyte"
	_ "dag-stream/operator/dag-plugin-hubmsgthrift"
	_ "dag-stream/operator/dag-plugin-iodfacethrift"
	_ "dag-stream/operator/dag-plugin-kafkaconsumer"
	_ "dag-stream/operator/dag-plugin-kafkaproducer"
)
