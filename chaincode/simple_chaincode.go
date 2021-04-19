package main

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/hyperledger/fabric/core/chaincode/shim"
	pb "github.com/hyperledger/fabric/protos/peer"
)

var logger = shim.NewLogger("SimpleChaincode")

type SimpleChaincode struct {
}

func (cc *SimpleChaincode) Init(stub shim.ChaincodeStubInterface) pb.Response {
	logger.SetLevel(shim.LogDebug)
	logger.Info("SimpleChaincode.Init")
	return shim.Success(nil)
}

func (cc *SimpleChaincode) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	logger.Info("SimpleChaincode.Invoke")

	function, args := stub.GetFunctionAndParameters()
	logger.Debugf("function: %s", function)

	if function == "put" {
		return cc.put(stub, args)
	} else if function == "get" {
		return cc.get(stub, args)
	} else if function == "del" {
		return cc.del(stub, args)
	} else if function == "getByRange" {
		return cc.getByRange(stub, args)
	}

	message := fmt.Sprintf("unknown function name: %s, expected one of {get, put, del, getByRange}", function)
	logger.Error(message)
	return pb.Response{Status: 400, Message: message}
}

func (cc *SimpleChaincode) put(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	logger.Info("SimpleChaincode.put")

	if len(args) != 3 {
		message := fmt.Sprintf("wrong number of arguments: passed %d, expected %d", len(args), 3)
		logger.Error(message)
		return pb.Response{Status: 400, Message: message}
	}

	objType, key, value := args[0], args[1], args[2]
	logger.Debugf("type: %s, key: %s, value: %s", objType, key, value)

	compositeKey, err := createCompositeKey(stub, objType, key)
	if err != nil {
		message := fmt.Sprintf("unable to create a composite key: %s", err.Error())
		logger.Error(message)
		return shim.Error(message)
	}

	if err := stub.PutState(compositeKey, []byte(value)); err != nil {
		message := fmt.Sprintf("unable to put a key-value pair: %s", err.Error())
		logger.Error(message)
		return shim.Error(message)
	}

	logger.Info("SimpleChaincode.put exited successfully")
	return shim.Success(nil)
}

func (cc *SimpleChaincode) get(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	logger.Info("SimpleChaincode.get")

	if len(args) != 2 {
		message := fmt.Sprintf("wrong number of arguments: passed %d, expected %d", len(args), 2)
		logger.Error(message)
		return pb.Response{Status: 400, Message: message}
	}

	objType, key := args[0], args[1]
	logger.Debugf("type: %s, key: %s", objType, key)

	compositeKey, err := createCompositeKey(stub, objType, key)
	if err != nil {
		message := fmt.Sprintf("unable to create a composite key: %s", err.Error())
		logger.Error(message)
		return shim.Error(message)
	}

	valueBytes, err := stub.GetState(compositeKey)
	if err != nil {
		message := fmt.Sprintf("unable to get a value for the key %s: %s", key, err.Error())
		logger.Error(message)
		return shim.Error(message)
	}

	if valueBytes == nil {
		message := fmt.Sprintf("a value for the key %s not found", key)
		logger.Error(message)
		return pb.Response{Status: 404, Message: message}
	}

	logger.Info("SimpleChaincode.get exited successfully")
	return shim.Success(valueBytes)
}

func (cc *SimpleChaincode) del(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	logger.Info("SimpleChaincode.del")

	if len(args) != 2 {
		message := fmt.Sprintf("wrong number of arguments: passed %d, expected %d", len(args), 2)
		logger.Error(message)
		return pb.Response{Status: 400, Message: message}
	}

	objType, key := args[0], args[1]
	logger.Debugf("type: %s, key: %s", objType, key)

	compositeKey, err := createCompositeKey(stub, objType, key)
	if err != nil {
		message := fmt.Sprintf("unable to create a composite key: %s", err.Error())
		logger.Error(message)
		return shim.Error(message)
	}

	if err := stub.DelState(compositeKey); err != nil {
		message := fmt.Sprintf("unable to delete a pair associated with the key %s: %s", key, err.Error())
		logger.Error(message)
		return shim.Error(message)
	}

	logger.Info("SimpleChaincode.del exited successfully")
	return shim.Success(nil)
}

func (cc *SimpleChaincode) getByRange(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	logger.Info("SimpleChaincode.getByRange")

	if len(args) != 2 {
		message := fmt.Sprintf("wrong number of arguments: passed %d, expected %d", len(args), 2)
		logger.Error(message)
		return pb.Response{Status: 400, Message: message}
	}

	keyFrom, keyTo := args[0], args[1]
	logger.Debugf("range: [\"%s\", \"%s\")", keyFrom, keyTo)

	it, err := stub.GetStateByRange(keyFrom, keyTo)
	if err != nil {
		message := fmt.Sprintf("unable to get an iterator over the range [\"%s\", \"%s\"): %s",
			keyFrom, keyTo, err.Error())
		logger.Error(message)
		return shim.Error(message)
	}
	defer it.Close()

	type queryResult struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	var entries = []queryResult{}
	for it.HasNext() {
		response, err := it.Next()
		if err != nil {
			message := fmt.Sprintf("unable to get the next element: %s", err.Error())
			logger.Error(message)
			return shim.Error(message)
		}

		entry := queryResult{
			Key:   response.Key,
			Value: string(response.Value),
		}
		logger.Debugf("entry: (%s, %s)", entry.Key, entry.Value)

		entries = append(entries, entry)
	}

	result, err := json.Marshal(entries)
	if err != nil {
		message := fmt.Sprintf("unable to marshal the result: %s", err.Error())
		logger.Error(message)
		return shim.Error(message)
	}

	logger.Info("SimpleChaincode.getByRange exited successfully")
	return shim.Success(result)
}

func createCompositeKey(stub shim.ChaincodeStubInterface, objType, key string) (string, error) {
	if key == "" {
		return "", errors.New("key must be a non-empty string")
	}

	if objType == "" {
		return key, nil
	}

	return stub.CreateCompositeKey(objType, []string{key})
}

func main() {
	err := shim.Start(new(SimpleChaincode))
	if err != nil {
		fmt.Printf("Error starting SimpleChaincode: %s", err)
	}
}
