package mr

import (
	"fmt"
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
	Maptasks []Task
	Redtasks []Task
}
type Task struct {
	index    int
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

func (c *Coordinator) GetReduce(args *GetReduceArgs, reply *GetReduceReply) error {
	reply.NReduce = globalReduce
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	var task *Task
	if c.nMap > 0 {
		fmt.Println("map1")
		task = c.selectTask(c.Maptasks, args.Workerid)
	} else if c.nReduce > 0 {
		fmt.Println("reduce")
		task = c.selectTask(c.Redtasks, args.Workerid)
	} else {
		fmt.Println("exit")
		task = &Task{-1, "EXIT", "", "", -1}
	}
	reply.File = task.file
	reply.TaskType = task.taskType
	reply.Id = task.index
	c.mu.Unlock()
	go c.checkTask(task)
	return nil
}

func (c *Coordinator) ReportDone(arg *ReportDoneArgs, reply *ReportDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var t *Task
	if arg.TaskType == "MAP" {
		fmt.Println("map")
		t = &c.Maptasks[arg.Id]
	} else if arg.TaskType == "RED" {
		t = &c.Redtasks[arg.Id]
	} else {
		t = nil
	}
	if t != nil && t.status == "run" {
		t.status = "done"
		fmt.Println(c.nMap)
		if t.taskType == "MAP" && c.nMap > 0 {
			fmt.Println(c.nMap)
			c.nMap--
			if c.nMap == 0 {
				fmt.Println("All map tasks done. Starting reduce phase.")
			}
		} else if t.taskType == "RED" && c.nReduce > 0 {
			c.nReduce--
		}
	}
	reply.Succ = c.nMap == 0 && c.nReduce == 0

	return nil
}

func (c *Coordinator) selectTask(queue []Task, workerId int) *Task {
	var task *Task
	for i := 0; i < len(queue); i++ {
		if queue[i].status == "ns" {
			task = &queue[i]
			task.status = "run"
			task.workerId = workerId

			return task
		}
	}
	return &Task{-1, "NOT", "", "", -1}
}

func (c *Coordinator) checkTask(t *Task) {
	if t.taskType != "MAP" && t.taskType != "RED" {
		return
	}
	time.Sleep(30 * time.Second)

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
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Println(c.nMap, c.nReduce)
	ret = c.nMap == 0 && c.nReduce == 0

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files: files, nReduce: nReduce}
	n := len(files)
	c.nMap = n
	c.Maptasks = make([]Task, 0, n)
	c.Redtasks = make([]Task, 0, nReduce)
	globalReduce = nReduce
	for i := 0; i < n; i++ {
		task := Task{i, "MAP", "ns", files[i], -1}
		c.Maptasks = append(c.Maptasks, task)
	}
	for i := 0; i < nReduce; i++ {
		task := Task{i, "RED", "ns", "", -1}
		c.Redtasks = append(c.Redtasks, task)
	}

	// Your code here.

	c.server()
	return &c
}
