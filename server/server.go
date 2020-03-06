package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	zmq "github.com/pebbe/zmq4"
	"github.com/shamaton/msgpack"
)

var Tr100020Rep map[string]interface{} = map[string]interface{}{
	"ret-cd":  0,
	"ret-msg": "",
	"tr100020": map[string]interface{}{
		"group-name": "",
		"count":      0,
		"accounts":   [100]string{},
	},
}

var Tr100021Rep map[string]interface{} = map[string]interface{}{
	"ret-cd":  0,
	"ret-msg": "",
}

var Tr900001Rep map[string]interface{} = map[string]interface{}{
	"ret-cd":  0,
	"ret-msg": "",
}

var handler map[string]interface{} = map[string]interface{}{
	"TR100020": AccountSearch,
	"TR100021": InsertAndDelete,
	"TR900001": SaveToken,
}

var Service string
var logger *log.Logger
var programName string = "server"

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
	router, _ := context.NewSocket(zmq.ROUTER)

	defer router.Close()

	router.Bind("tcp://*:5557")

	logger.Println("Account Test is starting...")
	fmt.Println("Account Test is starting...")

	for {
		recv, err := router.RecvMessageBytes(0)

		if err != nil {
			logger.Panic(err)
		}

		var recvMap map[string]interface{}

		err = msgpack.Decode(recv[1], &recvMap)
		if err != nil {
			logger.Panic(err)
		}
		logger.Printf("Received message from %v : %v", recv[0], recvMap)

		//display message
		fmt.Println("Request Service : ", recvMap["service"])

		Service = recvMap["service"].(string)
		result := handler[Service].(func(map[string]interface{}) interface{})(recvMap)
		fmt.Println(result)
		message := SetReply(result)

		packed, err := msgpack.Encode(message)

		if err != nil {
			logger.Panic(err)
		}

		router.SendMessage(recv[0], packed)

		fmt.Println("Reply Done")
	}
}

func AccountSearch(recvMap map[string]interface{}) interface{} {
	mongoClient := ConnectMongo()

	dbName := "tempDB"
	collectionName := "account"

	filter := bson.M{
		"htsid":   recvMap["htsid"],
		"nextkey": recvMap["nextkey"],
		"grpnm":   recvMap["grpnm"],
	}

	collection := mongoClient.Database(dbName).Collection(collectionName)

	cursor, err := collection.Find(context.TODO(), filter)

	if err != nil {
		logger.Panic(err)
	}

	ctx, _ := context.WithTimeout(context.Background(), 15*time.Second)

	var resultArray []bson.M

	for cursor.Next(ctx) {
		var result bson.M

		err := cursor.Decode(&result)

		if err != nil {
			logger.Panic(err)
		} else {
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
		logger.Panic(err)
	}

	err = client.Ping(context.TODO(), nil)

	return client
}

func SetReply(result interface{}) map[string]interface{} {
	//have to redesign
	//get format from other .go file and set data to format
	if Service == "TR100020" {
		if len(result.([]bson.M)) != 0 {
			Tr100020Rep["ret-cd"] = 1
			Tr100020Rep["ret-msg"] = "Search success"

			var tr100020Array []map[string]interface{}
			for k, _ := range result.([]bson.M) {
				var tr100020 map[string]interface{} = map[string]interface{}{
					"group-name": result.([]bson.M)[k]["grpnm"],
					"count":      len(result.([]bson.M)[k]["stklist"].(bson.A)),
					"accounts":   result.([]bson.M)[k]["stklist"],
				}
				tr100020Array = append(tr100020Array, tr100020)
			}
			Tr100020Rep["tr100020"] = tr100020Array
			logger.Println("set Tr100020Rep : ", Tr100020Rep)
			return Tr100020Rep
		} else if len(result.([]bson.M)) == 0 {
			Tr100020Rep["ret-cd"] = -1
			Tr100020Rep["ret-msg"] = "Not matched data"
			Tr100020Rep["tr100020"] = nil

			logger.Println("set Tr100020Rep : ", Tr100020Rep)
			return Tr100020Rep
		}
	} else if Service == "TR100021" {
		switch result.(type) {
		case *mongo.InsertOneResult:
			if result.(*mongo.InsertOneResult) != nil {
				Tr100021Rep["ret-cd"] = 1
				Tr100021Rep["ret-msg"] = fmt.Sprintf("result : %v", result.(*mongo.InsertOneResult))
				return Tr100021Rep
			}
		case *mongo.DeleteResult:
			if result.(*mongo.DeleteResult) != nil {
				Tr100021Rep["ret-cd"] = 1
				Tr100021Rep["ret-msg"] = fmt.Sprintf("result : %v", result.(*mongo.DeleteResult))
				return Tr100021Rep
			}
		}
	} else if Service == "TR900001" {
		switch result.(type) {
		case *mongo.InsertOneResult:
			if result.(*mongo.InsertOneResult) != nil {
				Tr100021Rep["ret-cd"] = 1
				Tr100021Rep["ret-msg"] = fmt.Sprintf("result : %v", result.(*mongo.InsertOneResult))
				return Tr100021Rep
			}
		}
	}
	return nil
}

func InsertAndDelete(recvMap map[string]interface{}) interface{} {
	var result interface{}
	if recvMap["proctp"] == "I" {
		result = InsertData(recvMap)
		logger.Println("Insert Result : ", result)
	} else if recvMap["proctp"] == "D" {
		result = DeleteData(recvMap)
		logger.Println("Delete Result : ", result)
	}

	return result
}

func InsertData(recvMap map[string]interface{}) *mongo.InsertOneResult {
	mongoClient := ConnectMongo()
	dbName := "tempDB"
	collectionName := "account"

	collection := mongoClient.Database(dbName).Collection(collectionName)

	data := bson.M{
		"htsid":   recvMap["htsid"],
		"nextkey": "",
		"grpnm":   recvMap["grpnm"],
		"stklist": recvMap["stklist"],
	}

	result, err := collection.InsertOne(context.TODO(), data)

	if err != nil {
		logger.Panic(err)
	}

	defer mongoClient.Disconnect(context.TODO())
	return result
}

func DeleteData(recvMap map[string]interface{}) *mongo.DeleteResult {
	mongoClient := ConnectMongo()
	dbName := "tempDB"
	collectionName := "account"

	collection := mongoClient.Database(dbName).Collection(collectionName)

	keys := bson.M{
		"htsid": recvMap["htsid"],
		"grpnm": recvMap["grpnm"],
	}

	result, err := collection.DeleteMany(context.TODO(), keys)

	if err != nil {
		logger.Panic(err)
	}

	defer mongoClient.Disconnect(context.TODO())
	return result

}

func SaveToken(recv map[string]interface{}) interface{} {
	mongoClient := ConnectMongo()
	dbName := "tempDB"
	collectionName := "token"

	collection := mongoClient.Database(dbName).Collection(collectionName)

	data := bson.M{
		"token":  recv["token"],
		"userID": recv["userID"],
		"grpnm":  "",
	}

	result, err := collection.InsertOne(context.TODO(), data)

	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	fmt.Println(recv)

	return result
}
