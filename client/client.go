package main

import (
	"fmt"
	"strings"

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

var dealer *zmq.Socket

func main() {
	context, _ := zmq.NewContext()
	dealer, _ = context.NewSocket(zmq.DEALER)

	dealer.Connect("tcp://localhost:5557")

	DisplayMenu()

	defer dealer.Close()

}

func DisplayMenu() {
	for {
		var selector int
		fmt.Println("Menu Process")
		fmt.Println("1. Search Data (tr100020)")
		fmt.Println("2. Insert or Delete Data (tr100021)")

		fmt.Println("Select menu :")
		fmt.Scanf("%d", &selector)

		if selector == 1 {
			SearchData()
		} else if selector == 2 {
			InsertDeleteData()
		}
	}
}

func SearchData() {
	var from string = "david"
	var htsid string

	fmt.Println("Account Search Process")
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

func InsertDeleteData() {
	for {
		fmt.Println("Select Process Type")
		fmt.Println("Insert(I) or Delete(D)")
		var selector string
		fmt.Scanf("%s", &selector)
		selector = strings.ToUpper(selector)

		if selector == "I" || selector == "INSERT" {
			Tr100021Req["proctp"] = "I"
			break
		} else if selector == "D" || selector == "DELETE" {
			Tr100021Req["proctp"] = "D"
			break
		}
	}

	var from string = "david"
	var htsid string
	var grpnm string

	fmt.Println("htsid : ")
	fmt.Scanf("%s", &htsid)
	fmt.Println("grpnm : ")
	fmt.Scanf("%s", &grpnm)

	Tr100021Req["from"] = from
	Tr100021Req["htsid"] = htsid
	Tr100021Req["grpnm"] = grpnm

	if Tr100021Req["proctp"] == "I" {
		fmt.Println("Set Account List to add DB")
		var stklist []string
		for {
			fmt.Println("Account Number : ")
			var account string
			fmt.Scanf("%s", &account)
			if strings.ToUpper(account) == "EXIT" {
				break
			} else if len(stklist) == 99 {
				fmt.Println("The List reach full size")
				break
			}
			stklist = append(stklist, account)
		}

		fmt.Println("list size : ", len(stklist))

		Tr100021Req["stklist"] = stklist
		Tr100021Req["stkcnt"] = len(stklist)
	} else if Tr100021Req["proctp"] == "D" {
		Tr100021Req["stklist"] = nil
		Tr100021Req["stkcnt"] = 0
	}

	fmt.Println("Tr100021Req : ", Tr100021Req)

	packed, err := msgpack.Encode(Tr100021Req)

	if err != nil {
		panic(err)
	}

	n, err := dealer.SendMessage(packed)

	if err != nil {
		panic(err)
	}

	fmt.Println("sent n :", n)

	var recvMap map[string]interface{}

	recv, err := dealer.RecvMessageBytes(0)

	if err != nil {
		panic(err)
	}

	err = msgpack.Decode(recv[0], &recvMap)

	if err != nil {
		panic(err)
	}

	fmt.Println(recvMap)

}
