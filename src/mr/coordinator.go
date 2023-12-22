package mr

import (
	"encoding/gob"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"time"

	"6.5840/mr/models"
)

type Coordinator struct {
	// Your definitions here.
	mapTaskList    *models.TaskList
	reduceTaskList *models.TaskList
	done           bool
	nReduce        int
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	// Your code here.
	return c.done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Register task types for rpc
	gob.RegisterName("MapTask", &models.MapTask{})
	gob.RegisterName("ReduceTask", &models.ReduceTask{})

	// Your code here.
	mapTaskList := models.NewTaskList(len(files))

	for i, filename := range files {
		mapTaskList.AddTask(&models.MapTask{ID: i, Status: models.Pending, Filename: filename})
	}

	reduceTaskList := models.NewTaskList(nReduce)

	for i := 0; i < nReduce; i++ {
		reduceTaskList.AddTask(&models.ReduceTask{ID: i, Status: models.Pending, NMap: len(files)})
	}

	c := Coordinator{mapTaskList: &mapTaskList, reduceTaskList: &reduceTaskList, nReduce: nReduce}

	c.server()
	return &c
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	var task *models.Task
	var taskList *models.TaskList

	if !c.mapTaskList.AllCompleted() {
		task = c.mapTaskList.GetNextPendingTask()
		taskList = c.mapTaskList
		if task != nil {
			reply.Task = *task
		}
	} else if !c.reduceTaskList.AllCompleted() {
		task = c.reduceTaskList.GetNextPendingTask()
		taskList = c.reduceTaskList
		if task != nil {
			reply.Task = *task
		}
	}

	reply.NReduce = c.nReduce

	go func() {
		time.Sleep(10 * time.Second)

		if task != nil {
			taskList.ResetIfNotComplete(task)
		}
	}()

	return nil
}

func GetPermFileName(tempFileName string) string {
	suffixIndex := strings.LastIndex(tempFileName, "-")

	return tempFileName[:suffixIndex]
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	var taskList *models.TaskList
	task := args.Task

	_, isMapTask := task.(*models.MapTask)

	if isMapTask {
		taskList = c.mapTaskList
	} else {
		taskList = c.reduceTaskList
	}

	// if task wasn't already completed, complete and rename files
	if taskList.CompleteTask(&task) {
		for _, tempFileName := range args.Files {
			// for map tasks, temp file names have format mr-<map task id>-<reduce task id>-<random str>
			// for reduce tasks, mr-<reduce task id>-<random str>
			os.Rename(tempFileName, GetPermFileName(tempFileName))
		}
	} else {
		for _, tempFileName := range args.Files {
			os.Remove(GetPermFileName(tempFileName))
		}
	}

	if c.reduceTaskList.AllCompleted() {
		c.done = true
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
