package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	reply := callMasterRequest()
	for {
		//fmt.Printf("Receive from master: %v\n", reply)
		switch reply.TaskType {
		case MapTask:
			intermediate := mapTask(mapf, reply.Input, reply.TaskID)
			writeIntermediate(intermediate, reply.NReduce, reply.TaskID)
			reply = callMapTaskFinished(reply.TaskID)
		case ReduceTask:
			reduceTask(reducef, reply.TaskID)
			reply = callReduceTaskFinished(reply.TaskID)
		case Done:
			//fmt.Printf("Worker %d close\n", os.Getpid())
			return
		}
	}

	//CallExample()
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//

func callMasterRequest() TaskReply {
	args := WorkerRequest{}
	args.RequestType = Idle
	args.Id = os.Getpid()
	reply := TaskReply{}
	result := call("Master.HandleRequest", &args, &reply)
	if !result {
		reply.TaskType = Done
	}
	return reply
}

func callMapTaskFinished(taskID int) TaskReply {
	args := WorkerRequest{}
	args.Id = os.Getpid()
	args.RequestType = FinishMap
	args.TaskID = taskID
	reply := TaskReply{}
	result := call("Master.HandleRequest", &args, &reply)
	if !result {
		reply.TaskType = Done
	}
	return reply
}

func callReduceTaskFinished(taskID int) TaskReply {
	args := WorkerRequest{}
	args.Id = os.Getpid()
	args.RequestType = FinishReduce
	args.TaskID = taskID
	reply := TaskReply{}
	result := call("Master.HandleRequest", &args, &reply)
	if !result {
		reply.TaskType = Done
	}
	return reply
}

func mapTask(mapf func(string, string) []KeyValue, fileName string, mapTaskID int) []KeyValue {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	return mapf(fileName, string(content))
}

func writeIntermediate(intermediate []KeyValue, nReduce int, mapTaskID int) {
	maps := make(map[int][]KeyValue)
	for i := 0; i < nReduce; i++ {
		maps[i] = []KeyValue{}
	}
	for _, kv := range intermediate {
		index := ihash(kv.Key) % nReduce
		maps[index] = append(maps[index], kv)
	}
	for reduseID, kvs := range maps {
		fileName := "mr-" + strconv.Itoa(mapTaskID) + "-" + strconv.Itoa(reduseID)
		file, _ := ioutil.TempFile("", "mr-")
		//file, _ := os.Create(fileName)
		enc := json.NewEncoder(file)
		for _, kv := range kvs {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf(err.Error())
			}
		}
		os.Rename(file.Name(), fileName)
	}
}

func reduceTask(reducef func(string, []string) string, reduceTaskID int) {
	allfiles, err := ioutil.ReadDir(".")
	if err != nil {
		log.Fatal(err)
	}
	pattern := "mr-*-" + strconv.Itoa(reduceTaskID)
	kva := []KeyValue{}
	for _, f := range allfiles {
		if b, _ := filepath.Match(pattern, f.Name()); b {
			file, err := os.Open(f.Name())
			if err != nil {
				log.Fatalf("cannot open %v", f.Name())
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
		}
	}
	kvMap := make(map[string][]string)
	for _, kv := range kva {
		kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
	}
	oname := "mr-out-" + strconv.Itoa(reduceTaskID)
	ofile, _ := ioutil.TempFile("", "mr-out-")
	for key, values := range kvMap {
		output := reducef(key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", key, output)
	}
	os.Rename(ofile.Name(), oname)
	ofile.Close()
}

func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
