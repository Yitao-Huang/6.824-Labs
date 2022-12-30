package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
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

type worker struct {
	wid 		int
	mapfunc 	func(string, string) []KeyValue
	reducefunc 	func(string, []string) string
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	w := worker{mapfunc: mapf, reducefunc: reducef}

	w.register()

	// run and receive tasks
	for {
		task := w.requestTask()
		log.Printf("Worker %d get task %+v", w.wid, task)
		if task.TaskType == Map {
			w.DoMap(task)
		} else {
			w.DoReduce(task)
		}
	}
}

func (w *worker) DoMap(task Task) {
	content, _ := ioutil.ReadFile(task.Filename)

	kva := w.mapfunc(task.Filename, string(content))
	reduce := make([][]KeyValue, task.NReduce)

	for _, kv := range kva {
		idx := ihash(kv.Key) % task.NReduce
		reduce[idx] = append(reduce[idx], kv)
	}

	for idx, r := range reduce {
		filename := "mr-" + strconv.Itoa(task.Num) + "-" + strconv.Itoa(idx)
		file, err := os.Create(filename)
		if err != nil {
			w.doneTask(task, false)
			return
		}
		enc := json.NewEncoder(file)
		for _, kv := range r {
			err := enc.Encode(&kv)
			if err != nil {
				w.doneTask(task, false)
				return
			}
		}
		file.Close()
	}

	w.doneTask(task, true)
}

func (w *worker) DoReduce(task Task) {
	maps := make(map[string][]string)

	for idx := 0; idx < task.NMap; idx++ {
		filename := "mr-" + strconv.Itoa(idx) + "-" + strconv.Itoa(task.Num)
		file, err := os.Open(filename)
		if err != nil {
			w.doneTask(task, false)
			return
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}

	resFileName := "mr-out-" + strconv.Itoa(task.Num)
	ofile, err := os.Create(resFileName)
	if err != nil {
		w.doneTask(task, false)
		return
	}

	for key, values := range maps {
		fmt.Fprintf(ofile, "%v %v\n", key,  w.reducefunc(key, values))
	}

	w.doneTask(task, true)
}

// RPC impl

func (w *worker) register() {
	args := RegisterArgs{}
	reply := RegisterReply{}
	ok := call("Master.Register", args, &reply)
	if ok {
		w.wid = reply.WID
	}
}

func (w *worker) requestTask() Task {
	args := GetTaskArgs{WID: w.wid}
	reply := GetTaskReply{}
	ok := call("Master.GetATask", args, &reply)
	if !ok {
		os.Exit(1)
	}
	return reply.Task
}

func (w *worker) doneTask(task Task, success bool) {
	args := FinishTaskArgs{TaskNum: task.Num, Type: task.TaskType, WID: w.wid, Done: success}
	reply := FinishTaskReply{}
	ok := call("Master.FinishTask", args, &reply)
	if !ok {
		os.Exit(1)
	}
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
