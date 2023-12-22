package models

import (
	"sync"
)

type TaskStatus int

const (
	Pending TaskStatus = iota
	InProgress
	Complete
)

type Task interface {
	GetID() int
	GetStatus() TaskStatus
	SetStatus(TaskStatus)
}

type MapTask struct {
	ID       int
	Status   TaskStatus
	Filename string
}

func (t *MapTask) GetID() int {
	return t.ID
}

func (t *MapTask) GetStatus() TaskStatus {
	return t.Status
}

func (t *MapTask) SetStatus(status TaskStatus) {
	t.Status = status
}

type ReduceTask struct {
	ID     int
	Status TaskStatus
	NMap   int
}

func (t *ReduceTask) GetID() int {
	return t.ID
}

func (t *ReduceTask) GetStatus() TaskStatus {
	return t.Status
}

func (t *ReduceTask) SetStatus(status TaskStatus) {
	t.Status = status
}

type TaskList struct {
	taskList     []Task
	mutex        sync.Mutex
	allCompleted bool
}

func (tl *TaskList) AllCompleted() bool {
	tl.mutex.Lock()
	defer tl.mutex.Unlock()

	if tl.allCompleted {
		return true
	}

	for _, task := range tl.taskList {
		if task.GetStatus() != Complete {
			return false
		}
	}

	tl.allCompleted = true
	return true
}

func (tl *TaskList) AddTask(task Task) {
	tl.taskList = append(tl.taskList, task)
}

func (tl *TaskList) GetNextPendingTask() *Task {
	tl.mutex.Lock()
	defer tl.mutex.Unlock()

	for _, task := range tl.taskList {
		if task.GetStatus() == Pending {
			task.SetStatus(InProgress)
			return &task
		}
	}

	return nil
}

func (tl *TaskList) ResetIfNotComplete(taskToReset *Task) {
	tl.mutex.Lock()
	defer tl.mutex.Unlock()

	task := tl.taskList[(*taskToReset).GetID()]

	if task.GetStatus() != Complete {
		task.SetStatus(Pending)
	}
}

func (tl *TaskList) CompleteTask(taskToComplete *Task) bool {
	tl.mutex.Lock()
	defer tl.mutex.Unlock()

	task := tl.taskList[(*taskToComplete).GetID()]

	if task.GetStatus() != Complete {
		task.SetStatus(Complete)
		return true
	}
	return false
}

func NewTaskList(numTasks int) TaskList {
	tasks := make([]Task, 0)

	return TaskList{taskList: tasks}
}
