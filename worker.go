package mr

import (
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "os"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

var nReduce int
var nMap int

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.
	var workerId int64 = time.Now().UnixNano()
	//fmt.Printf("workerId = %d :: coordSockName = %s\n",
	//	workerId, coordSockName)
	err := RegisterWorker(workerId)
	if err != nil {
		//fmt.Printf("workerId = %d :: register error = %v", workerId, err)
		os.Exit(1)
	}

	for {
		tskObj, err := GetTask(workerId)
		if err != nil {
			//fmt.Printf("GetTask Error: %v\n", err)
			os.Exit(0)
		}
		if tskObj == nil {
			//fmt.Printf("WorkerId = %d :: No work available now\n", workerId)
			time.Sleep(1 * time.Second)
		} else {
			//fmt.Printf("WorkerId = %d :: Worker is running\n", workerId)
			if tskObj.TaskType == mapTask {
				filename := tskObj.MapInput
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
					err := SendErrorDoneTask(workerId, *tskObj)
					if err != nil {
						log.Fatalf("SendErrorDoneTask Error: %v\n", err)
						os.Exit(0)
					}
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
					err := SendErrorDoneTask(workerId, *tskObj)
					if err != nil {
						log.Fatalf("SendErrorDoneTask Error: %v\n", err)
						os.Exit(0)
					}
				}
				file.Close()
				kva := mapf(filename, string(content))
				err = emitMap(kva, tskObj.NReduce, tskObj.MapId)
				if err != nil {
					log.Fatalf("emitMap Error: %v\n", err)
					err := SendErrorDoneTask(workerId, *tskObj)
					if err != nil {
						log.Fatalf("SendErrorDoneTask Error: %v\n", err)
						os.Exit(0)
					}
				}
			} else {
				filenames := tskObj.ReduceInputs
				var reduceInput map[string][]string = make(map[string][]string)
				for _, filename := range filenames {
					file, err := os.Open(filename)
					if err != nil {
						log.Fatalf("cannot open %v", filename)
						err := SendErrorDoneTask(workerId, *tskObj)
						if err != nil {
							log.Fatalf("SendErrorDoneTask Error: %v\n", err)
							os.Exit(0)
						}
					}
					content, err := ioutil.ReadAll(file)
					if err != nil {
						log.Fatalf("cannot open %v", filename)
						err := SendErrorDoneTask(workerId, *tskObj)
						if err != nil {
							log.Fatalf("SendErrorDoneTask Error: %v\n", err)
							os.Exit(0)
						}
					}
					file.Close()
					words := strings.Fields(string(content))
					for i := 0; i < len(words)-1; i += 2 {
						key := words[i]
						value := words[i+1]
						reduceInput[key] = append(reduceInput[key], value)
					}
				}
				err = emitReduce(reduceInput, tskObj.ReduceId, reducef)
				if err != nil {
					log.Fatalf("emitReduce Error: %v\n", err)
					err := SendErrorDoneTask(workerId, *tskObj)
					if err != nil {
						log.Fatalf("SendErrorDoneTask Error: %v\n", err)
						os.Exit(0)
					}
				}
			}
			SendDoneTask(workerId, *tskObj, nil, "")
		}
		//time.Sleep(1 * time.Second)
	}
}

func emitReduce(reduceInput map[string][]string, reduceId int, reducef func(string, []string) string) error {
	outputFile := fmt.Sprintf("mr-out-%v", reduceId)
	outputFd, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer outputFd.Close()
	for key, value := range reduceInput {
		out := reducef(key, value)
		fmt.Fprintf(outputFd, "%v %v\n", key, out)
	}
	return nil
}

func emitMap(kva []KeyValue, nReduce int, mapId int) error {
	var files []*os.File
	for i := 0; i < nReduce; i++ {
		filename := fmt.Sprintf("mr-%d-%d", mapId, i)
		file, err := os.Create(filename)
		if err != nil {
			return err
		}
		files = append(files, file)
	}
	for _, kv := range kva {
		fileid := ihash(kv.Key) % nReduce
		file := files[fileid]
		_, err := fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
		if err != nil {
			return err
		}
	}
	return nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func RegisterWorker(workerId int64) error {
	args := RegisterArgs{}
	args.WorkerId = workerId
	reply := RegisterReply{}
	for _ = range 3 {
		ok := call("Coordinator.Register", &args, &reply)
		if ok {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return errors.New("RegisterWorker Error")
}

func GetTask(workerId int64) (*TaskObject, error) {
	//fmt.Println("GetTask rpc called")
	args := GetTaskArgs{}
	args.WorkerId = workerId
	reply := GetTaskReply{}
	for _ = range 3 {
		ok := call("Coordinator.GetTask", &args, &reply)
		if ok {
			//fmt.Printf("GetTask reply: %v\n", reply)
			if reply.IsWorkAvailable {
				return &(reply.TaskObject), nil
			}
			return nil, nil
		}
		time.Sleep(1 * time.Second)
	}
	return nil, errors.New("get task failed")
}

func SendDoneTask(workerId int64, taskObject TaskObject, mapOutputs []string, reduceOutput string) error {
	args := DoneTaskArgs{}
	reply := DoneTaskReply{}
	args.WorkerId = workerId
	args.TaskId = taskObject.TaskId
	args.IsSuccessful = true
	if taskObject.TaskType == mapTask {
		args.MapOutputs = mapOutputs
	} else {
		args.ReduceOutput = reduceOutput
	}
	for _ = range 3 {
		ok := call("Coordinator.DoneTask", &args, &reply)
		if ok {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return errors.New("send task failed")
}

func SendErrorDoneTask(workerId int64, taskObject TaskObject) error {
	args := DoneTaskArgs{}
	args.WorkerId = workerId
	args.TaskId = taskObject.TaskId
	args.IsSuccessful = false
	reply := DoneTaskReply{}
	for _ = range 3 {
		ok := call("Coordinator.DoneTask", &args, &reply)
		if ok {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return errors.New("send task failed")
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
//	func call(rpcname string, args interface{}, reply interface{}) bool {
//		// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
//		c, err := rpc.DialHTTP("unix", coordSockName)
//		if err != nil {
//			log.Fatal("dialing:", err)
//		}
//		defer c.Close()
//
//		if err := c.Call(rpcname, args, reply); err == nil {
//			return true
//		}
//		log.Printf("%d: call failed err %v", os.Getpid(), err)
//		return false
//	}
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		//log.Printf("dialing failed: %v", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		//log.Printf("%d: RPC %s failed: %v",
		//	os.Getpid(), rpcname, err)
		return false
	}

	return true
}
