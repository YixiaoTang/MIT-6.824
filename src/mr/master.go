package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	Done         = "Done"
	Idle         = "idle"
	FinishMap    = "Finish Map"
	FinishReduce = "Finish Reduce"
	Unfinished   = 0
	Processing   = 1
	Finished     = 2
	MapTask      = "Map"
	ReduceTask   = "Reducd"
)

type Master struct {
	// Your definitions here.
	workers          map[int]int
	nReduce          int
	mutex            sync.Mutex
	files            []string
	mapStatus        map[int]int // the status for each input file
	processingMap    map[int]time.Time
	reduceStatus     map[int]int
	processingReduce map[int]time.Time
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) HandleRequest(args *WorkerRequest, reply *TaskReply) error {
	// if _, exist := m.workers[args.Id]; !exist {
	// 	m.workers[args.Id] = len(m.workers) + 1
	// }
	// fmt.Printf("Receive request from worker **%d**, request type is %s.\n", m.workers[args.Id], args.RequestType)
	if args.RequestType != Idle {
		m.setTaskFinished(args.RequestType, args.TaskID)
	}
	m.timeoutCheck()
	for m.isMapTasksFinished() == false {
		mapTaskID, file := m.findUnassignedMapTask()
		if mapTaskID != -1 {
			reply.TaskType = MapTask
			reply.Input = file
			reply.TaskID = mapTaskID
			reply.NReduce = m.nReduce
			return nil
		}
		time.Sleep(time.Millisecond * 100)
		m.timeoutCheck()
	}

	for m.isReduceTasksFinished() == false {
		reduceTaskID := m.findUnassignedReduceTask()
		if reduceTaskID != -1 {
			reply.TaskType = ReduceTask
			reply.TaskID = reduceTaskID
			return nil
		}
		time.Sleep(time.Millisecond * 100)
		m.timeoutCheck()
	}
	reply.TaskType = Done
	return nil

}

func (m *Master) setTaskFinished(taskType string, taskID int) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	switch taskType {
	case FinishMap:
		m.mapStatus[taskID] = Finished
	case FinishReduce:
		m.reduceStatus[taskID] = Finished
	}
}

func (m *Master) isMapTasksFinished() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for _, status := range m.mapStatus {
		if status != Finished {
			return false
		}
	}
	return true
}

func (m *Master) isReduceTasksFinished() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for _, status := range m.reduceStatus {
		if status != Finished {
			return false
		}
	}
	return true
}

func (m *Master) findUnassignedMapTask() (int, string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for index, file := range m.files {
		if m.mapStatus[index] == Unfinished {
			m.mapStatus[index] = Processing
			m.processingMap[index] = time.Now()
			return index, file
		}
	}
	return -1, ""
}

func (m *Master) findUnassignedReduceTask() int {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for index, status := range m.reduceStatus {
		if status == Unfinished {
			m.reduceStatus[index] = Processing
			m.processingReduce[index] = time.Now()
			return index
		}
	}
	return -1
}

func (m *Master) timeoutCheck() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for index, status := range m.mapStatus {
		if status == Processing && time.Now().Sub(m.processingMap[index]) > time.Second*10 {
			m.mapStatus[index] = Unfinished
			//fmt.Printf("Map task %d failed.\n", index)
		}
	}
	for index, status := range m.reduceStatus {
		if status == Processing && time.Now().Sub(m.processingReduce[index]) > time.Second*10 {
			m.reduceStatus[index] = Unfinished
			//fmt.Printf("Reduce task %d failed.\n", index)
		}
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {

	// Your code here.

	return m.isMapTasksFinished() && m.isReduceTasksFinished()
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		files:            files,
		workers:          make(map[int]int),
		mapStatus:        make(map[int]int),
		processingMap:    make(map[int]time.Time),
		reduceStatus:     make(map[int]int),
		processingReduce: make(map[int]time.Time),
	}
	m.nReduce = nReduce
	for i := range files {
		m.mapStatus[i] = Unfinished
	}
	for i := 0; i < nReduce; i++ {
		m.reduceStatus[i] = Unfinished
	}

	// for k, v := range m.files {
	// 	fmt.Println(k, v)
	// }

	// Your code here.
	m.server()
	return &m
}
