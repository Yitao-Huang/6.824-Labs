package mr

import "time"

const (
	Ready   	= 0
	Queued		= 1
	InProgress 	= 2
	Completed  	= 3
	Err			= 4
)

const (
	Map    = "Map"
	Reduce = "Reduce"
)

type Task struct {
	NReduce			int
	NMap			int
	TaskType		string
	Filename		string
	Num				int
}

type TaskStatus struct {
	Status			int
	WID				int
	StartTime		time.Time
}