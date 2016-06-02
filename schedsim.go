package main

import (
	"fmt"
	"log"
	"time"
)

var (
	configHZ             = 100
	timerPeriod          = time.Duration((int(1*time.Second) / configHZ))
	curr                 *task
	sched_latency        = time.Duration(24 * time.Millisecond)
	sched_min_gran       = time.Duration(3 * time.Millisecond)
	sched_wakeup_gran    = time.Duration(4 * time.Millisecond)
	NO_WAKEUP_PREEMPTION = true
)

type policy int

const (
	NORMAL = 0
	BATCH  = 1
	IDLE   = 2
)

var batchPreemptsLc = 0
var lcPreemptsBatch = 0

type state int

func (s state) String() string {
	switch s {
	case 0:
		return "RUNNABLE"
	case 1:
		return "RUNNING"
	case 2:
		return "SLEEPING"
	}
	return "unknownl"
}

const (
	RUNNABLE = 0
	RUNNING  = 1
	SLEEPING = 2
)

type task struct {
	name     string
	state    state
	policy   policy
	vruntime time.Duration
	weight   int

	// workload
	burn  time.Duration
	sleep time.Duration

	wakeup                chan int
	preempt               chan int
	transition            chan int
	exec_start            time.Time
	sum_exec_runtime      time.Duration
	prev_sum_exec_runtime time.Duration
}

func (t *task) String() string {
	return t.name
}

var lc = &task{
	name:       "lc",
	state:      RUNNABLE,
	policy:     NORMAL,
	weight:     100000,
	burn:       100 * time.Millisecond,
	sleep:      0 * time.Millisecond,
	wakeup:     make(chan int),
	preempt:    make(chan int),
	transition: make(chan int),
}

var batch = &task{
	name:       "batch",
	state:      RUNNABLE,
	policy:     IDLE,
	weight:     1,
	burn:       100 * time.Millisecond,
	sleep:      0 * time.Millisecond,
	wakeup:     make(chan int),
	preempt:    make(chan int),
	transition: make(chan int),
}

var total_weight = lc.weight + batch.weight

const NICE_0_LOAD = 1024

func resched_curr() {
	log.Println("-> resched_curr curr =", curr)
	if curr.state == RUNNING {
		curr.preempt <- 1
		log.Println("preempt", curr)
		<-curr.transition
		if curr.state == RUNNING {
			panic("expected not RUNNING got " + curr.state.String())
		}
	}
	next := pick_next_task()
	if next != nil {
		set_next_entity(next)
		log.Println("<- resched_curr: curr = ", curr, " next = ", next)
	}
}

func update_curr() {
	delta_exec := time.Since(curr.exec_start)
	curr.exec_start = time.Now()

	curr.sum_exec_runtime += delta_exec
	delta_vruntime := calc_delta_fair(delta_exec, curr.weight)
	log.Println("update_curr: curr =", curr, "delta vruntime = ", delta_vruntime)
	curr.vruntime += delta_vruntime
}

func check_preempt_wakeup(curr, new *task) {
	log.Println("check_preempt_wakeup")
	if curr == new {
		return
	}
	if curr.policy == IDLE && new.policy != IDLE {
		log.Println("check_preempt_wakeup: resched becuase curr is IDLE")
		resched_curr()
		return
	}

	if new.policy == IDLE || new.policy == BATCH {
		return
	}

	if NO_WAKEUP_PREEMPTION {
		return
	}

	update_curr()

	if wakeup_preempt_entity(curr, new) {
		log.Println("check_preempt_wakeup: resched because new waited too long")
		log.Println("PREEMPT ", curr)
		resched_curr()
	}

}

func wakeup_preempt_entity(curr, new *task) bool {
	log.Println("wakeup_preempt_entity", curr, new)
	vdiff := curr.vruntime - new.vruntime
	gran := calc_delta_fair(sched_wakeup_gran, new.weight)
	log.Println("wakeup_preempt_entity: vdiff = ", vdiff, " gran = ", gran)
	return vdiff > gran
}

func __calc_delta(delta time.Duration, weight, load_weight int) time.Duration {
	return time.Duration(int(delta) * weight / load_weight)
}

func calc_delta_fair(delta time.Duration, load_weight int) time.Duration {
	return __calc_delta(delta, NICE_0_LOAD, load_weight)
}

func pick_next_task() *task {
	if lc.state != SLEEPING && lc.vruntime < batch.vruntime {
		return lc
	} else if batch.state != SLEEPING && batch.vruntime < lc.vruntime {
		return batch
	}
	return nil
}

func sched_slice(task *task) time.Duration {
	return __calc_delta(sched_latency, task.weight, total_weight)
}

func check_preempt_tick() {
	log.Println("check_preempt_tick: curr = ", curr)
	ideal_runtime := sched_slice(curr)
	log.Println("check_preempt_tick: ideal_runtime = ", ideal_runtime)
	delta_exec := curr.sum_exec_runtime - curr.prev_sum_exec_runtime
	log.Println("check_preempt_tick: delta_exec = ", delta_exec)
	// run too long
	if delta_exec > ideal_runtime {
		log.Println("PREEMTP", curr, " - run too long -> resched_curr")
		resched_curr() // PREEMPT
		return
	}

	// not to short
	if delta_exec > sched_min_gran {
		return // DONT PREEMPT
	}

	first := pick_next_task()
	if first != nil {
		log.Println("check_preempt_tick: first =", first)
		delta := curr.vruntime - first.vruntime
		log.Println("check_preempt_tick: delta =", delta)
		if delta > ideal_runtime {
			log.Println("check_preempt_tick: -> resched_curr becuase first should run now")
			resched_curr()
		}
	}
}

func scheduler_tick() {
	log.Println("scheduler_tick")
	update_curr()
	check_preempt_tick()
}

func timerClock() {
	for range time.Tick(timerPeriod) {
		scheduler_tick()
	}
}

var woken = make(chan *task)

func burn(task *task) {

	go func() {
		for {
			<-task.wakeup
			task.state = RUNNING
			log.Println("running:", task)
			task.transition <- 1
			select {
			case <-task.preempt:
				log.Println("task", task, "PREEMPTED!")
				if task == batch {
					lcPreemptsBatch += 1
				} else {
					batchPreemptsLc += 1
				}
				task.state = SLEEPING
				task.transition <- 1
			case <-time.After(task.burn):
			}

			if task.sleep > time.Duration(0) {
				task.state = SLEEPING
				log.Println("sleeping:", task)
				time.Sleep(task.sleep)
			}
			task.state = RUNNABLE
			log.Println("runnable:", task)
			woken <- task
		}
	}()
}

func set_next_entity(task *task) {
	log.Println("set_next_entity", task)
	curr = task
	task.prev_sum_exec_runtime = task.sum_exec_runtime
	task.exec_start = time.Now()
	log.Println("wakeup", task)
	task.wakeup <- 1
	<-task.transition
}

func main() {
	fmt.Println("timerPeriod =", timerPeriod)
	go func() {
		for new := range woken {
			check_preempt_wakeup(curr, new)
		}
	}()

	burn(lc)
	burn(batch)

	set_next_entity(lc)

	go timerClock()

	time.Sleep(1 * time.Second)

	fmt.Println(lc, "vruntime", lc.vruntime)
	fmt.Println(batch, "vruntime", batch.vruntime)

	fmt.Println(lc, "sum_exec_time", lc.sum_exec_runtime)
	fmt.Println(batch, "sum_exec_time", batch.sum_exec_runtime)
	fmt.Println("batchPreemptsLc", batchPreemptsLc)
	fmt.Println("lcPreemptsBatch", lcPreemptsBatch)
}
