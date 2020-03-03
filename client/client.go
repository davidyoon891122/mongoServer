package main

import (
	"fmt"

	"github.com/shamaton/msgpack"

	zmq "github.com/pebbe/zmq4"
)

var Tr100020Req map[string]interface{} = map[string]interface{}{
	"from":    "",
	"service": "TR100020",
	"htsid":   "",
	"nextkey": "",
}

var Tr100020Rep map[string]interface{} = map[string]interface{}{
	"ret-cd":  0,
	"ret-msg": "",
	"tr100020": map[string]interface{}{
		"group-name": "",
		"count":      0,
		"accounts":   [100]string{},
	},
}

var Tr100021Req map[string]interface{} = map[string]interface{}{
	"from":    "",
	"service": "TR100021",
	"stkcnt":  0,
	"proctp":  "",
	"grpnm":   "",
	"stklist": [100]string{},
}

var Tr100021Rep map[string]interface{} = map[string]interface{}{
	"ret-cd":  0,
	"ret-msg": "",
}

func main() {
	context, _ := zmq.NewContext()
	dealer, _ := context.NewSocket(zmq.DEALER)

	defer dealer.Close()

	dealer.Connect("tcp://localhost:5557")

	for {
		var from string
		var htsid string

		fmt.Println("Account Search Process")
		fmt.Println("from : ")
		fmt.Scanf("%s", &from)
		fmt.Println("htsid : ")
		fmt.Scanf("%s", &htsid)

		Tr100020Req["from"] = from
		Tr100020Req["htsid"] = htsid
		Tr100020Req["nextkey"] = ""

		fmt.Println(Tr100020Req)

		packed, err := msgpack.Encode(Tr100020Req)

		if err != nil {
			panic(err)
		}

		n, err := dealer.SendMessage(packed)

		if err != nil {
			panic(err)
		}

		fmt.Println("sent n : ", n)

		recv, err := dealer.RecvMessageBytes(0)

		var recvMap map[string]interface{}

		err = msgpack.Decode(recv[0], &recvMap)

		if err != nil {
			panic(err)
		}

		fmt.Println("recvMap : ", recvMap)

	}

}
