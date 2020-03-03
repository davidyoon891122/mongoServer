package main

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	zmq "github.com/pebbe/zmq4"
	"github.com/shamaton/msgpack"
)

var Tr100020Req map[string]interface{} = map[string]interface{}{
	"from":     "",
	"service":  "TR100020",
	"htsid":    "",
	"nextkey:": "",
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

var Service string

func main() {
	context, _ := zmq.NewContext()
	router, _ := context.NewSocket(zmq.ROUTER)

	defer router.Close()

	router.Bind("tcp://*:5557")

	fmt.Println("Account Test is starting...")

	for {
		recv, err := router.RecvMessageBytes(0)

		if err != nil {
			panic(err)
		}

		var recvMap map[string]interface{}

		err = msgpack.Decode(recv[1], &recvMap)
		if err != nil {
			panic(err)
		}

		fmt.Println(recvMap)

		Tr100020Req = recvMap

		Service = recvMap["service"].(string)
		if recvMap["service"] == "TR100020" {
			resultArray := AccountSearch()

			SetReply(resultArray)

			packed, err := msgpack.Encode(Tr100020Rep)

			if err != nil {
				panic(err)
			}

			router.SendMessage(recv[0], packed)

		}

	}
}

func AccountSearch() []bson.M {
	mongoClient := ConnectMongo()

	dbName := "tempDB"
	collectionName := "account"

	filter := bson.M{
		"htsid":   Tr100020Req["htsid"],
		"nextkey": Tr100020Req["nextkey"],
	}

	collection := mongoClient.Database(dbName).Collection(collectionName)

	cursor, err := collection.Find(context.TODO(), filter)

	if err != nil {
		panic(err)
	}

	ctx, _ := context.WithTimeout(context.Background(), 15*time.Second)

	var resultArray []bson.M

	for cursor.Next(ctx) {
		var result bson.M

		err := cursor.Decode(&result)

		if err != nil {
			panic(err)
		} else {
			fmt.Println("result: ", result)
			resultArray = append(resultArray, result)
		}
	}

	defer cursor.Close(ctx)
	defer mongoClient.Disconnect(context.TODO())

	return resultArray

}

func ConnectMongo() *mongo.Client {
	clientOption := options.Client().ApplyURI("mongodb://localhost:27017")
	client, err := mongo.Connect(context.TODO(), clientOption)

	if err != nil {
		panic(err)
	}

	err = client.Ping(context.TODO(), nil)

	fmt.Println("Connected to MongoDB!")

	return client
}

func SetReply(result []bson.M) {
	if Service == "TR100020" {
		if len(result) != 0 {
			Tr100020Rep["ret-cd"] = 1
			Tr100020Rep["ret-msg"] = "Search success"
			Tr100020Rep["tr100020"] = map[string]interface{}{
				"group-name": "",
				"count":      len(result[0]["stklist"].(bson.A)),
				"accounts":   result[0]["stklist"],
			}
			fmt.Println("set Tr100020Rep : ", Tr100020Rep)
		} else if len(result) == 0 {
			Tr100020Rep["ret-cd"] = -1
			Tr100020Rep["ret-msg"] = "Not matched data"
			Tr100020Rep["tr100020"] = nil

			fmt.Println("set Tr100020Rep : ", Tr100020Rep)
		} // be made case multiple results from DB.
	} else if Service == "TR100021" {
		//
	}
}
