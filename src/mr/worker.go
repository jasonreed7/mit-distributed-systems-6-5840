package mr

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"

	"6.5840/mr/models"
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

	// Register task types for rpc
	gob.RegisterName("MapTask", &models.MapTask{})
	gob.RegisterName("ReduceTask", &models.ReduceTask{})

	// Your worker implementation here.
	for {
		task, nReduce, err := callAssignTask()

		if err != nil {
			// If AssignTask call fails, can't reach coordinator
			// Assume coordinator has finished and shut down worker
			break
		}

		if task == nil {
			time.Sleep(1 * time.Second)
			continue
		}

		mapTask, isMapTask := task.(*models.MapTask)

		if isMapTask {
			processMapTask(mapTask, mapf, nReduce)
		} else {
			reduceTask := task.(*models.ReduceTask)

			processReduceTask(reduceTask, reducef, reduceTask.NMap, nReduce)
		}
	}

}

func processMapTask(task *models.MapTask, mapf func(string, string) []KeyValue, nReduce int) {
	filename := task.Filename

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	encoders := make([]*json.Encoder, nReduce)
	outFiles := make([]*os.File, nReduce)

	for i := 0; i < nReduce; i++ {
		outFileName := fmt.Sprintf("mr-%d-%d-", task.ID, i)
		outFiles[i], err = os.CreateTemp(".", outFileName)
		defer outFiles[i].Close()

		if err != nil {
			log.Fatalf("cannot create map outfile %v", outFileName)
		}
		encoders[i] = json.NewEncoder(outFiles[i])
	}

	for _, kv := range kva {
		reducer := ihash(kv.Key) % nReduce
		encoders[reducer].Encode(kv)
	}

	callCompleteTask(task, outFiles)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func processReduceTask(task *models.ReduceTask, reducef func(string, []string) string, nMap int, nReduce int) {
	inFileNames := make([]string, nMap)

	for i := 0; i < nMap; i++ {
		inFileNames[i] = fmt.Sprintf("mr-%d-%d", i, task.ID)
	}

	kva := make([]KeyValue, 0)

	for _, inFileName := range inFileNames {
		inFile, err := os.Open(inFileName)
		if err != nil {
			log.Fatalf("cannot open %v", inFileName)
		}

		dec := json.NewDecoder(inFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

		inFile.Close()
	}

	sort.Sort(ByKey(kva))

	oname := fmt.Sprintf("mr-out-%d-", task.ID)
	ofile, _ := os.CreateTemp(".", oname)

	//
	// call Reduce on each distinct key in kva[],
	// and print the result to mr-out.
	//
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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	callCompleteTask(task, []*os.File{ofile})

	ofile.Close()
}

func callAssignTask() (models.Task, int, error) {

	// declare an argument structure.
	args := AssignTaskArgs{}

	// declare a reply structure.
	reply := AssignTaskReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		return reply.Task, reply.NReduce, nil
	} else {
		// fmt.Printf("AssignTask call failed!\n")
		return nil, reply.NReduce, fmt.Errorf("AssignTask call failed")
	}
}

func callCompleteTask(task models.Task, outFiles []*os.File) error {

	// declare an argument structure.
	args := CompleteTaskArgs{}
	args.Task = task

	fileNames := make([]string, len(outFiles))

	for i, outFile := range outFiles {
		fileNames[i] = outFile.Name()
	}

	args.Files = fileNames

	// declare a reply structure.
	reply := CompleteTaskReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.CompleteTask", &args, &reply)
	if ok {
		return nil
	} else {
		fmt.Printf("CompleteTask call failed!\n")
		return fmt.Errorf("CompleteTask call failed")
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
		return false
		// log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
