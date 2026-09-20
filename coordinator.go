package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var workerTimeOut time.Duration = time.Duration(10) * time.Second

type worker struct {
	workerId      int64
	lastHeartbeat time.Time
}

type status int

const (
	statusSuccess status = iota
	statusPending
	statusWorking
)

func getStatus(status status) string {
	if status == statusWorking {
		return "working"
	} else if status == statusPending {
		return "pending"
	} else if status == statusSuccess {
		return "success"
	}
	return "unknown"
}

type task struct {
	TaskId       int64
	MapId        int
	ReduceId     int
	nReduce      int
	TaskType     taskType
	Status       status
	MapInput     string
	mapOutputs   []string
	ReduceInputs []string
	ReduceOutput string
	workerId     int64
}

func (t *task) getTaskObject() TaskObject {
	var obj TaskObject
	obj.TaskId = t.TaskId
	obj.ReduceId = t.ReduceId
	obj.MapId = t.MapId
	obj.TaskType = t.TaskType
	obj.MapInput = t.MapInput
	obj.ReduceInputs = t.ReduceInputs
	obj.NReduce = t.nReduce
	return obj
}

type Coordinator struct {
	workers map[int64]worker
	tasks   map[int64]task

	tasksMutex   sync.Mutex
	workersMutex sync.Mutex
	rpcMutex     sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	//fmt.Println("Coordinator :: Register rpc called")
	//_, ok := c.workers[args.WorkerId]
	c.rpcMutex.Lock()
	defer c.rpcMutex.Unlock()

	_, ok := c.getWorker(args.WorkerId)
	if !ok {
		c.addWorker(args.WorkerId)
	} else {
		c.updateWorkerTimeStamp(args.WorkerId)
	}
	return nil
}
func (c *Coordinator) getMapLeftTask() (int64, bool) {
	//c.tasksMutex.Lock()
	//defer c.tasksMutex.Unlock()
	retTaskId := int64(0)
	anyMapLeft := false
	for _, tsk := range c.tasks {
		if tsk.TaskType == mapTask && tsk.Status != statusSuccess {
			anyMapLeft = true
		}
		if tsk.TaskType == mapTask && tsk.Status == statusPending {
			retTaskId = tsk.TaskId
		}
	}
	return retTaskId, anyMapLeft
}

func (c *Coordinator) getReduceLeftTask() int64 {
	//c.tasksMutex.Lock()
	//defer c.tasksMutex.Unlock()
	retTaskId := int64(-1)
	for _, tsk := range c.tasks {
		if tsk.TaskType == reduceTask && tsk.Status == statusPending {
			retTaskId = tsk.TaskId
		}
	}
	return retTaskId
}

func (c *Coordinator) allMapsDone() bool {
	allMapsDone := true
	for _, tsk := range c.tasks {
		if tsk.TaskType == mapTask && tsk.Status != statusSuccess {
			allMapsDone = false
			break
		}
	}
	return allMapsDone
}

func (c *Coordinator) GetPendingMapTask() int64 {
	retVal := int64(-1)
	for _, tsk := range c.tasks {
		if tsk.TaskType == mapTask && tsk.Status == statusPending {
			retVal = tsk.TaskId
			break
		}
	}
	return retVal
}
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	//fmt.Println("Coordinator :: GetTask rpc called")
	c.rpcMutex.Lock()
	defer c.rpcMutex.Unlock()
	c.updateWorkerTimeStamp(args.WorkerId)
	retTaskId := c.GetPendingMapTask()
	if retTaskId >= 0 {
		retTask, _ := c.getTask(retTaskId)
		c.updateTaskToWorking(retTaskId, args.WorkerId)
		reply.TaskObject = retTask.getTaskObject()
		reply.IsWorkAvailable = true

		//fmt.Printf(
		//	"Coordinator :: GetTask return map task = %v\n",
		//	reply.TaskObject,
		//)
		return nil
	}
	allMapsDone := c.allMapsDone()
	if !allMapsDone {
		reply.IsWorkAvailable = false
		return nil
	}

	retTaskId = c.getReduceLeftTask()
	if retTaskId < 0 {
		reply.IsWorkAvailable = false
		return nil
	}
	retTask, ok := c.getTask(retTaskId)
	retTask.ReduceInputs = c.createReduceTaskInput(retTask)
	c.updateTask(retTask)
	if ok {
		reply.TaskObject = retTask.getTaskObject()
		reply.IsWorkAvailable = true
		c.updateTaskToWorking(retTaskId, args.WorkerId)
		//fmt.Printf("Coordinator :: GetTask rpc return taskObject = %v\n", retTask.getTaskObject())
	} else {
		reply.IsWorkAvailable = false
		//fmt.Println("Coordinator :: GetTask rpc return no Work Available")
	}
	return nil
}

func (c *Coordinator) createReduceTaskInput(tsk task) []string {
	reduceId := tsk.ReduceId
	var output []string
	//c.tasksMutex.Lock()
	//defer c.tasksMutex.Unlock()
	for _, t := range c.tasks {
		if t.TaskType == mapTask {
			MapId := t.MapId
			output = append(output, fmt.Sprintf("mr-%v-%v", MapId, reduceId))
		}
	}
	return output
}

func (c *Coordinator) updateTaskToWorking(taskId int64, workerId int64) {
	tsk, ok := c.getTask(taskId)
	if !ok {
		return
	}
	tsk.workerId = workerId
	tsk.Status = statusWorking
	c.updateTask(tsk)
}

func (c *Coordinator) getTask(taskId int64) (task, bool) {
	//c.tasksMutex.Lock()
	//defer c.tasksMutex.Unlock()
	task, ok := c.tasks[taskId]
	return task, ok
}

func (c *Coordinator) updateTask(task task) {
	//c.tasksMutex.Lock()
	//defer c.tasksMutex.Unlock()
	c.tasks[task.TaskId] = task
}

func (c *Coordinator) DoneTask(args *DoneTaskArgs, reply *DoneTaskReply) error {
	//fmt.Println("Coordinator :: DoneTask rpc called")
	c.rpcMutex.Lock()
	defer c.rpcMutex.Unlock()
	c.updateWorkerTimeStamp(args.WorkerId)
	tsk, ok := c.getTask(args.TaskId)
	if ok {
		if !args.IsSuccessful {
			tsk.Status = statusPending
			c.updateTask(tsk)
			return nil
		}
		tsk.Status = statusSuccess
		if tsk.TaskType == mapTask {
			tsk.mapOutputs = args.MapOutputs
		} else {
			tsk.ReduceOutput = args.ReduceOutput
		}
		c.updateTask(tsk)
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	//fmt.Println("Coordinator :: Done called")
	//fmt.Printf("Coordinator :: TaskLeft\n")
	ret := true
	c.rpcMutex.Lock()
	defer c.rpcMutex.Unlock()
	c.updateTaskList()
	for _, tsk := range c.tasks {
		if tsk.Status != statusSuccess {
			ret = false
			if tsk.TaskType == mapTask {
				//fmt.Printf("MapTask::mapid-%d,status-%s\n", tsk.MapId, getStatus(tsk.Status))
			} else {
				//fmt.Printf("ReduceTask::ReduceId-%d,status-%s\n", tsk.ReduceId, getStatus(tsk.Status))
			}
		}
	}
	//fmt.Printf("\n\n\n\n\n")
	//fmt.Println("Coordinator:: isDone: ", ret)
	return ret
}

func (c *Coordinator) updateWorkerTimeStamp(workerId int64) {
	wrker, exists := c.getWorker(workerId)
	if !exists {
		// worker failed and some task in pending state with this workerId
		// task may continue pending in trench
		c.updateTaskFailureFromWorker(workerId)
		c.addWorker(workerId)
	} else {
		wrker.lastHeartbeat = time.Now()
		c.updateWorker(wrker)
	}
}

func (c *Coordinator) updateWorker(worker worker) {
	//c.workersMutex.Lock()
	//defer c.workersMutex.Unlock()
	c.workers[worker.workerId] = worker
}

func (c *Coordinator) getWorker(workerId int64) (worker, bool) {
	//c.workersMutex.Lock()
	//defer c.workersMutex.Unlock()
	wkr, ok := c.workers[workerId]
	return wkr, ok
}

func (c *Coordinator) addWorker(workerId int64) {
	//c.workersMutex.Lock()
	//defer c.workersMutex.Unlock()
	c.workers[workerId] = worker{
		workerId:      workerId,
		lastHeartbeat: time.Now(),
	}
}

// checks if worker heartbeat is greater than 10s
func (c *Coordinator) updateTaskList() {
	c.updateWorkerList()
	//c.tasksMutex.Lock()
	//defer c.tasksMutex.Unlock()
	for key, tsk := range c.tasks {
		if tsk.Status == statusWorking {
			if _, exists := c.getWorker(tsk.workerId); !exists {
				tsk.Status = statusPending
				tsk.workerId = 0
				// TODO: to add lock
				c.tasks[key] = tsk
			}
		}
	}
}

func (c *Coordinator) updateTaskFailureFromWorker(workerId int64) {
	//c.tasksMutex.Lock()
	//defer c.tasksMutex.Unlock()
	for key, tsk := range c.tasks {
		if tsk.Status == statusWorking && tsk.workerId == workerId {
			tsk.Status = statusPending
			tsk.workerId = 0
			c.tasks[key] = tsk
		}
	}
}

func (c *Coordinator) updateWorkerList() {
	//c.workersMutex.Lock()
	//defer c.workersMutex.Unlock()
	for _, worker := range c.workers {
		if time.Since(worker.lastHeartbeat) > workerTimeOut {
			// TODO: Add lock
			delete(c.workers, worker.workerId)
		}
	}

}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	//var nMap int = len(files)
	//fmt.Println("Coordinator :: nMap: ", nMap, " nReduce: ", nReduce, " files: ", files)

	var taskId int64 = 1
	c.tasks = map[int64]task{}
	c.workers = map[int64]worker{}
	for idx, file := range files {
		c.tasks[taskId] = task{
			TaskId:   taskId,
			MapId:    idx,
			nReduce:  nReduce,
			TaskType: mapTask,
			Status:   statusPending,
			MapInput: file,
			workerId: 0,
		}
		taskId++
	}
	for i := 0; i < nReduce; i++ {
		c.tasks[taskId] = task{
			TaskId:   taskId,
			ReduceId: i,
			TaskType: reduceTask,
			Status:   statusPending,
			workerId: 0,
		}
		taskId++
	}
	//fmt.Println("Coordinator :: Added all tasks to coordinator")
	c.server(sockname)
	return &c
}
