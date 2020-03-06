package main

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/shamaton/msgpack"

	zmq "github.com/pebbe/zmq4"
)

//add grpnm for searching
var Tr100020Req map[string]interface{} = map[string]interface{}{
	"from":    "",
	"service": "TR100020",
	"htsid":   "",
	"nextkey": "",
	"grpnm":   "",
}

var Tr100021Req map[string]interface{} = map[string]interface{}{
	"from":    "",
	"service": "TR100021",
	"stkcnt":  0,
	"proctp":  "",
	"grpnm":   "",
	"stklist": [100]string{},
}

var dealer *zmq.Socket
var logger *log.Logger
var programName string = "client"

func SetLogger() {
	currentDirectory, _ := os.Getwd()
	logPath := "/logs/" + programName + ".log"
	f, err := os.OpenFile(currentDirectory+logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}

	logger = log.New(f, "INFO : ", log.Ldate|log.Ltime|log.Lshortfile)
}

func main() {
	SetLogger()
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

		time.Sleep(3 * time.Second)
	}
}

func SearchData() {
	var from string = "david"
	var htsid string
	var grpnm string

	fmt.Println("Account Search Process")
	fmt.Println("htsid : ")
	fmt.Scanf("%s", &htsid)
	fmt.Println("grpnm : ")
	fmt.Scanf("%s", &grpnm)

	Tr100020Req["from"] = from
	Tr100020Req["htsid"] = htsid
	Tr100020Req["nextkey"] = ""
	Tr100020Req["grpnm"] = grpnm
	fmt.Println("Service Request : ", Tr100020Req["service"])
	logger.Println("Tr10002Req :", Tr100020Req)

	packed, err := msgpack.Encode(Tr100020Req)

	if err != nil {
		panic(err)
	}

	_, err = dealer.SendMessage(packed)

	if err != nil {
		panic(err)
	}

	recv, err := dealer.RecvMessageBytes(0)

	var recvMap map[string]interface{}

	err = msgpack.Decode(recv[0], &recvMap)

	if err != nil {
		panic(err)
	}
	logger.Println("recvMap : ", recvMap)
	//display result
	fmt.Println("*******************************************************************************")
	fmt.Println("ret-cd : ", recvMap["ret-cd"])
	fmt.Println("ret-msg : ", recvMap["ret-msg"])
	// fmt.Println("tr100020 : ", recvMap["tr100020"])
	mapArray := recvMap["tr100020"]

	switch mapArray.(type) {
	case []interface{}:
		for k, _ := range mapArray.([]interface{}) {
			fmt.Printf("%d. group-name : %v \n", k+1, mapArray.([]interface{})[k].(map[interface{}]interface{})["group-name"])
			fmt.Printf("    count : %v \n", mapArray.([]interface{})[k].(map[interface{}]interface{})["count"])
			fmt.Printf("    accounts : %v \n", mapArray.([]interface{})[k].(map[interface{}]interface{})["accounts"])
		}
		fmt.Println("*******************************************************************************")
	case nil:
		fmt.Printf("tr100020 : nil\n")
		fmt.Println("*******************************************************************************")
	}
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

	fmt.Println("Service Request : ", Tr100021Req["service"])

	if Tr100021Req["proctp"] == "I" {
		fmt.Println("Set Account List to add DB")
		var stklist []string
		for {
			fmt.Println("Account Number(000C00000000) : ")

			var account string

			r := regexp.MustCompile("(([0-9]{3})+C+([0-9]{8}))")
			fmt.Scanf("%s", &account)
			account = strings.ToUpper(account)

			if account == "EXIT" {
				break
			} else if len(stklist) == 99 {
				fmt.Println("The List reach full size")
				break
			} else if r.MatchString(account) == false {
				fmt.Println("Not matched account format")
				continue
			}
			stklist = append(stklist, account)
			fmt.Println("account is saved.")

		}

		fmt.Println("list size : ", len(stklist))

		Tr100021Req["stklist"] = stklist
		Tr100021Req["stkcnt"] = len(stklist)
	} else if Tr100021Req["proctp"] == "D" {
		Tr100021Req["stklist"] = nil
		Tr100021Req["stkcnt"] = 0
	}
	fmt.Println("Tr100021Req : ", Tr100021Req)
	logger.Println("Tr100021Req : ", Tr100021Req)

	packed, err := msgpack.Encode(Tr100021Req)

	if err != nil {
		logger.Panic(err)
	}

	_, err = dealer.SendMessage(packed)

	if err != nil {
		logger.Panic(err)
	}

	var recvMap map[string]interface{}

	recv, err := dealer.RecvMessageBytes(0)

	if err != nil {
		logger.Panic(err)
	}

	err = msgpack.Decode(recv[0], &recvMap)

	if err != nil {
		logger.Panic(err)
	}

	fmt.Println(recvMap)
	logger.Println(recvMap)

	//display result
	fmt.Println("*******************************************************************************")
	fmt.Println("ret-cd : ", recvMap["ret-cd"])
	fmt.Println("ret-msg : ", recvMap["ret-msg"])
	fmt.Println("*******************************************************************************")

}
