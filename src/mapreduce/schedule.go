package mapreduce

import "fmt"
//import "strconv"

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	tasks := make(chan int, ntasks)
	for i:=0;i<ntasks;i++{
		tasks <- i
	}
	for {
		if phase == mapPhase {
			process(jobName,mapFiles,nReduce,phase,registerChan,tasks,len(tasks),n_other)
			//fmt.Println("Length of remaining tasks ", strconv.Itoa(len(tasks)))
		} else {
			process(jobName,mapFiles,len(tasks),phase,registerChan,tasks,len(tasks),n_other)
			//fmt.Println("Length of remaining tasks ", strconv.Itoa(len(tasks)))
		}
		if len(tasks) == 0 {
			break
		}
	}
	fmt.Printf("Schedule: %v done\n", phase)
}
func process(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string, tasks chan int, ntasks int, n_other int) {
	nworkers := make(chan int)
	for len(tasks) > 0 {
		j := <- tasks
			if phase == mapPhase {
				mapTask := DoTaskArgs{ jobName, mapFiles[j], mapPhase, j, n_other}
				go func() {
					address := <- registerChan
					status := call(address,"Worker.DoTask",mapTask,nil)
					if status {
						nworkers <- 1 // current worker finished
						registerChan <- address // reuse the idle worker
					} else {
						tasks <- j
						nworkers <- 0 // current worker failed
					}
				}()
			} else {
				reduceTask := DoTaskArgs{ jobName, "", reducePhase, j, n_other }
				go func() {
					address := <- registerChan
					status := call(address,"Worker.DoTask",reduceTask,nil)
					if status {
						nworkers <- 1 // current worker finished
						registerChan <- address // reuse the idle worker
					} else {
						tasks <- j
						nworkers <- 0 // current worker failed
					}
				}()
			}
	}
	for p:=0; p < ntasks; p++ {
		<-nworkers
	}
}
