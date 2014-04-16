package mapreduce
import "container/list"
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply;
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}


func (mr *MapReduce) RunMaster() *list.List {
	// Your code here

	//slice is different array, array's length is fixed, to be investigate
	//var mapChan, reduceChan = make([]chan bool, mr.nMap), make([]chan bool, mr.nReduce)
	var mapChan, reduceChan = make(chan int, mr.nMap), make(chan int, mr.nReduce)

	//define here, instead of outside RunMaster() could read mr.XX directly
	var send_map = func(worker string, ind int) bool{
		var jobArgs DoJobArgs
		var reply DoJobReply
		jobArgs.NumOtherPhase = mr.nReduce
		jobArgs.Operation = Map
		jobArgs.File = mr.file
		jobArgs.JobNumber = ind
		return call(worker, "Worker.DoJob", jobArgs, &reply)
	}

	var send_reduce = func(worker string, ind int) bool{
		var jobArgs DoJobArgs
		var reply DoJobReply
		jobArgs.NumOtherPhase = mr.nMap
		jobArgs.Operation = Reduce
		jobArgs.File = mr.file
		jobArgs.JobNumber = ind
		return call(worker, "Worker.DoJob", jobArgs, &reply)
	}

	for i := 0; i < mr.nMap; i++ {
		go func(ind int){
			for{
				var worker string
				var ok bool = false
				select {
				case worker = <-mr.idleChannel:
					ok = send_map(worker, ind)
				case worker = <-mr.registerChannel:
					ok = send_map(worker, ind)
				}
				if (ok){
					//the order of this couldn't change, otherwise the last several task get stuck
					mapChan <- ind
					mr.idleChannel <- worker
					return
				}
			}
		}(i)
	}

	for i := 0; i < mr.nMap; i++{
		<- mapChan
	}

	fmt.Println("Map is Done")

	for i := 0; i < mr.nReduce; i++{
		go func(ind int){
			for {
				var worker string
				var ok = false
				select {
				case worker = <- mr.idleChannel:
					ok = send_reduce(worker, ind)
				case worker = <- mr.registerChannel:
					ok = send_reduce(worker, ind)
				}
				if ok{
					reduceChan <- ind
					mr.idleChannel <- worker
					return
				}
			}
		}(i)
	}

	for i := 0; i < mr.nReduce; i++{
		<- reduceChan
	}

	fmt.Println("Reduce is Done")
	return mr.KillWorkers()
}
