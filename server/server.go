package main

import (
	"context"
	"fmt"
	"reflect"
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
	"proctp":  "", //Insert(I), Delete(D)
	"htsid":   "",
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

		if recvMap["service"] == "TR100020" {
			Tr100020Req = recvMap

			Service = recvMap["service"].(string)
			resultArray := AccountSearch()

			SetReply(resultArray)

			packed, err := msgpack.Encode(Tr100020Rep)

			if err != nil {
				panic(err)
			}

			router.SendMessage(recv[0], packed)
		} else if recvMap["service"] == "TR100021" {
			Tr100021Req = recvMap
			Service = recvMap["service"].(string)
			var result interface{}
			if recvMap["proctp"] == "I" {
				result = InsertData()
				fmt.Println("Insert Result : ", result)
			} else if recvMap["proctp"] == "D" {
				result = DeleteData()
				fmt.Println("Delete Result : ", result)
			}
			SetReply(result)

			packed, err := msgpack.Encode(Tr100021Rep)

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

	fmt.Println("Collection type : ", reflect.TypeOf(collection))

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

//convert []bson.M to interface{}
func SetReply(result interface{}) {
	if Service == "TR100020" {
		if len(result.([]bson.M)) != 0 {
			Tr100020Rep["ret-cd"] = 1
			Tr100020Rep["ret-msg"] = "Search success"
			Tr100020Rep["tr100020"] = map[string]interface{}{
				"group-name": "",
				"count":      len(result.([]bson.M)[0]["stklist"].(bson.A)),
				"accounts":   result.([]bson.M)[0]["stklist"],
			}
			fmt.Println("set Tr100020Rep : ", Tr100020Rep)
		} else if len(result.([]bson.M)) == 0 {
			Tr100020Rep["ret-cd"] = -1
			Tr100020Rep["ret-msg"] = "Not matched data"
			Tr100020Rep["tr100020"] = nil

			fmt.Println("set Tr100020Rep : ", Tr100020Rep)
		} // be made case multiple results from DB.
	} else if Service == "TR100021" {
		switch result.(type) {
		case *mongo.InsertOneResult:
			if result.(*mongo.InsertOneResult) != nil {
				Tr100021Rep["ret-cd"] = 1
				Tr100021Rep["ret-msg"] = fmt.Sprintf("result : %v", result.(*mongo.InsertOneResult))
			}
		case *mongo.DeleteResult:
			if result.(*mongo.DeleteResult) != nil {
				Tr100021Rep["ret-cd"] = 1
				Tr100021Rep["ret-msg"] = fmt.Sprintf("result : %v", result.(*mongo.DeleteResult))
			}
		}
	}
}

func InsertData() *mongo.InsertOneResult {
	mongoClient := ConnectMongo()
	dbName := "tempDB"
	collectionName := "account"

	collection := mongoClient.Database(dbName).Collection(collectionName)

	data := bson.M{
		"htsid":   Tr100021Req["htsid"],
		"nextkey": "",
		"grpnm":   Tr100021Req["grpnm"],
		"stklist": Tr100021Req["stklist"],
	}

	result, err := collection.InsertOne(context.TODO(), data)

	if err != nil {
		panic(err)
	}

	defer mongoClient.Disconnect(context.TODO())
	return result
}

func DeleteData() *mongo.DeleteResult {
	mongoClient := ConnectMongo()
	dbName := "tempDB"
	collectionName := "account"

	collection := mongoClient.Database(dbName).Collection(collectionName)

	keys := bson.M{
		"htsid": Tr100021Req["htsid"],
		"grpnm": Tr100021Req["grpnm"],
	}

	result, err := collection.DeleteOne(context.TODO(), keys)

	if err != nil {
		panic(err)
	}

	defer mongoClient.Disconnect(context.TODO())
	return result

}
