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

type Master struct {
	// Your definitions here.
	mu 				sync.Mutex
	workerSeq		int
	nReduce			int
	files 			[]string
	done			bool
	tasks			chan Task
	taskStatus		[]TaskStatus
	phase			string
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) Register(args *RegisterArgs, reply *RegisterReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.workerSeq += 1
	reply.WID = m.workerSeq

	return nil
}

func (m *Master) getTask(taskNum int, reduceNum int, taskType string) Task {
	task := Task{
		Num: taskNum,
		TaskType: taskType,
		Filename: "",
		NReduce: m.nReduce,
		NMap: len(m.files),
	}

	if taskType == Map {
		task.Filename = m.files[taskNum]
	}

	return task
}

func (m *Master) GetATask(args *GetTaskArgs, reply *GetTaskReply) error {
	if m.Done() {
		return nil
	}

	// get task
	task := <-m.tasks
	reply.Task = task

	m.mu.Lock()
	defer m.mu.Unlock()
	m.taskStatus[task.Num].Status = InProgress
	m.taskStatus[task.Num].StartTime = time.Now()
	m.taskStatus[task.Num].WID = args.WID

	return nil
}

func (m *Master) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if args.Type != m.phase || args.WID != m.taskStatus[args.TaskNum].WID {
		return nil
	}

	// mark task as finished
	if args.Done {
		m.taskStatus[args.TaskNum].Status = Completed
	} else {
		m.taskStatus[args.TaskNum].Status = Err
	}

	return nil
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
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.done
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.workerSeq = 0
	m.mu = sync.Mutex{}
	m.files = files
	m.nReduce = nReduce
	m.done = false

	if nReduce > len(files) {
		m.tasks = make(chan Task, nReduce)
	} else {
		m.tasks = make(chan Task, len(m.files))
	}

	m.initMapTasks()
	go func() {
		for !m.Done() {
			m.schedule()
			time.Sleep(time.Millisecond * 500)
		}
	}()

	m.server()
	return &m
}

func (m *Master) initMapTasks() {
	m.taskStatus = make([]TaskStatus, len(m.files))
	m.phase = Map
	
	for i := 0; i < len(m.files); i++ {
		m.taskStatus[i].Status = Ready
	}
}

func (m *Master) initReduceTasks() {
	m.taskStatus = make([]TaskStatus, m.nReduce)
	m.phase = Reduce

	for i := 0; i < m.nReduce; i++ {
		m.taskStatus[i].Status = Ready
	}
}

func (m *Master) schedule() {
	if m.Done() {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	
	finished := true
	for taskNum, status := range m.taskStatus {
		switch status.Status {
		case Ready:
			finished = false
			if m.phase == Map {
				m.tasks <- m.getTask(taskNum, -1, Map)
			} else {
				m.tasks <- m.getTask(taskNum, taskNum, Reduce)
			}
			m.taskStatus[taskNum].Status = Queued
		case Queued:
			finished = false
		case InProgress:
			finished = false
			if time.Since(m.taskStatus[taskNum].StartTime) > 10 * time.Second {
				if m.phase == Map {
					m.tasks <- m.getTask(taskNum, -1, Map)
				} else {
					m.tasks <- m.getTask(taskNum, taskNum, Reduce)
				}
				m.taskStatus[taskNum].Status = Queued
			}
		case Completed:
		case Err:
			finished = false
			if m.phase == Map {
				m.tasks <- m.getTask(taskNum, -1, Map)
			} else {
				m.tasks <- m.getTask(taskNum, taskNum, Reduce)
			}
			m.taskStatus[taskNum].Status = Queued
		default:
			panic("t.status err")
		}
	}

	if finished {
		if m.phase == Map {
			m.initReduceTasks()
		} else {
			m.done = true
		}
	}
}
