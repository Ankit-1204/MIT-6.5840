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

var globalReduce int

type Coordinator struct {
	mu       sync.Mutex
	files    []string
	nReduce  int
	nMap     int
	Maptasks []*Task
	Redtasks []*Task
}
type Task struct {
	taskType string
	status   string
	file     string
	workerId int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetReduce(rpcname string, args *getReduceArgs, reply *getReduceReply) {
	reply.nReduce = globalReduce
}

func (c *Coordinator) GetTask(rpcname string, args *GetTaskArgs, reply *GetTaskReply) {
	c.mu.Lock()
	var task *Task
	if c.nMap > 0 {
		task = c.selectTask(c.Maptasks, args.Workerid)
	} else if c.nReduce > 0 {
		task = c.selectTask(c.Redtasks, args.Workerid)
	} else {

		task = &Task{"EXIT", "", "", -1}
	}
	reply.file = task.file
	reply.taskType = task.taskType
	c.mu.Unlock()
	go c.checkTask(task)

}

func (c *Coordinator) selectTask(queue []*Task, workerId int) *Task {
	var task *Task
	for _, t := range queue {
		if t.status == "ns" {
			task = t
			t.status = "run"
			t.workerId = workerId
			return task
		}
	}
	return &Task{"NOT", "", "", -1}
}

func (c *Coordinator) checkTask(t *Task) {
	if t.taskType != "MAP" && t.taskType != "RED" {
		return
	}
	time.Sleep(15 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()

	if t.status == "run" {
		t.status = "ns"
		t.workerId = -1
	}

}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files: files, nReduce: nReduce}
	n := len(files)
	c.nMap = n
	globalReduce = nReduce
	for i := 0; i < n; i++ {
		task := Task{"MAP", "ns", files[i], -1}
		c.Maptasks = append(c.Maptasks, &task)
	}
	for i := 0; i < nReduce; i++ {
		task := Task{"RED", "ns", "", -1}
		c.Redtasks = append(c.Redtasks, &task)
	}

	// Your code here.

	c.server()
	return &c
}
