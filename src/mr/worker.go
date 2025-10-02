package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	redReply := getReduceReply{}
	GetReduce(&redReply)
	nReduce := redReply.nReduce
	var succ bool
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		getArg := GetTaskArgs{os.Getpid()}
		getReply := GetTaskReply{}
		err := GetTask(&getArg, &getReply)
		if err != nil {
			fmt.Println(err)
		}
		if getReply.taskType == "EXIT" {
			fmt.Println("All task complete")
			return
		} else if getReply.taskType == "MAP" {
			data, err := os.ReadFile(getReply.file)
			if err != nil {
				fmt.Println(err)
			}
			kva := mapf(getReply.file, string(data))
			err = writeMap(&kva, nReduce, getReply.id)
			if err != nil {
				fmt.Println(err)
			}
			succ, err = reportDone("MAP", getReply.id)
		} else if getReply.taskType == "RED" {
			kva, err := doReduce(nReduce, getReply.id)
			if err != nil {
				fmt.Println(err)
			}
			err = writeReduce(reducef, kva, getReply.id)
			if err != nil {
				fmt.Println(err)
			}
			succ, err = reportDone("RED", getReply.id)
		}
		if succ && err == nil {
			fmt.Println("tasks done")
			return
		}
		time.Sleep(15 * time.Second)
	}

}

func reportDone(taskType string, taskId int) (bool, error) {
	reportArg := ReportDoneArgs{os.Getpid(), taskType, taskId}
	reportReply := ReportDoneReply{}

	ok := call("Coordinator.ReportDone", reportArg, reportReply)
	if ok {
		// reply.Y should be 100.
		return reportReply.succ, nil
	} else {
		err := errors.New("get task error")
		return false, err
	}
}
func writeMap(kva *[]KeyValue, nReduce int, tid int) error {
	if kva == nil {
		return errors.New("no kva data")
	}
	pid := os.Getpid()
	dirname := "temp"
	err := os.MkdirAll(dirname, 0755)
	if err != nil {
		return err
	}
	files := make([]*os.File, 0, nReduce)
	encoders := make([]*json.Encoder, 0, nReduce)
	for i := 0; i < nReduce; i++ {
		filename := fmt.Sprintf("mr-%d-%d-%d", tid, i, pid)
		filepath := filepath.Join(dirname, filename)
		file, err := os.Create(filepath)
		if err != nil {
			fmt.Println("Error creating file:", err)
			continue
		}
		files = append(files, file)
		newEnc := json.NewEncoder(file)
		encoders = append(encoders, newEnc)
	}

	for _, d := range *kva {
		id := ihash(d.Key) % nReduce
		err := encoders[id].Encode(&d)
		if err != nil {
			return err
		}

	}
	for i, f := range files {
		tmpName := fmt.Sprintf("mr-%d-%d-%d", tid, i, pid)
		finalName := fmt.Sprintf("mr-%d-%d", tid, i)

		tmpPath := filepath.Join(dirname, tmpName)
		finalPath := filepath.Join(dirname, finalName)

		f.Close()

		if err := os.Rename(tmpPath, finalPath); err != nil {
			return fmt.Errorf("error renaming %s -> %s: %w", tmpPath, finalPath, err)
		}
	}
	return nil
}

func doReduce(nReduce int, rid int) ([]KeyValue, error) {
	dirname := "temp"
	pattern := filepath.Join(dirname, fmt.Sprintf("mr-*-%d", rid))
	match, err := filepath.Glob(pattern)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	var kva []KeyValue
	for _, f := range match {
		file, err := os.Open(f)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := decoder.Decode(&kv)
			if err == io.EOF {
				break
			} else if err != nil {
				fmt.Println(err)
				file.Close()
				return nil, err
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	return kva, nil
}

func writeReduce(reducef func(string, []string) string, kva []KeyValue, rid int) error {
	sort.Slice(kva, func(i, j int) bool { return kva[i].Key < kva[j].Key })
	filename := fmt.Sprintf("mr-out-%d", rid)
	file, err := os.Create(filename)
	if err != nil {
		fmt.Println(err)
	}
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(file, "%v %v\n", kva[i].Key, output)

		i = j
	}
	file.Close()
	return nil
}
func GetReduce(redReply *getReduceReply) error {
	redArg := getReduceArgs{}
	ok := call("Coordinator.GetReduce", redArg, redReply)
	if ok {
		// reply.Y should be 100.
		return nil
	} else {
		err := errors.New("get task error")
		return err
	}
}

func GetTask(getArg *GetTaskArgs, getReply *GetTaskReply) error {

	ok := call("Coordinator.GetTask", getArg, getReply)
	if ok {
		// reply.Y should be 100.
		return nil
	} else {
		err := errors.New("get task error")
		return err
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
