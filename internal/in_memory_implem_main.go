package main

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type JobFn func(job interface{})

type Worker struct {
	Job JobFn
}

type Queue struct {
	//JobQueue stores the objects that will be passed to the job function
	//note that both the job queue and job function use the interface{} type
	JobQueue   chan interface{}
	KillSignal chan bool
	Wg         sync.WaitGroup
}

func KillWorkers(q *Queue) {
	//ensures that all workers receive the kill signal and stop processing
	close(q.KillSignal)
}

var timeoutDefault int = 30000

func (w *Worker) ProcessJob(q *Queue, wId int) {
	for {
		select {
		//if there is a job in available in the queue
		case job := <-q.JobQueue:
			//add another select here with timeout logic
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutDefault)*time.Millisecond)

			//using a buffered channel avoids a goroutine leak
			//assume we use an unbuffered channel
			//imagine a scenario where the timeout is achieved first, so the logic under <- ctx.Done() is executed
			//in this case the goroutine (which is continuing to execute) would reach done <- true and block indefinitely (goroutine leak)
			//since there is no receiver to take the value from done. this would not block if using a buffered channel
			done := make(chan bool, 1)

			go func() {
				//call the job function with the job retrieved from job queue
				w.Job(job)
				done <- true
			}()
			select {
			case <-done:
				log.Printf("Job completed by worker %v\n", wId)
			case <-ctx.Done():
				log.Printf("Job timed out for worker %v\n", wId)
			}
			cancel()

		//if a value can be retrieved from the KillSignal channel, stop all workers
		case <-q.KillSignal:
			q.Wg.Done()
			return
		}
	}
}

func StartWorkers(numWorkers int, q *Queue, job JobFn) {
	q.Wg.Add(numWorkers)
	for i := 1; i <= numWorkers; i++ {
		w := Worker{Job: job}
		go w.ProcessJob(q, i)
	}
}

func (q *Queue) EnqueueJob(job interface{}) {
	q.JobQueue <- job
}

func main() {
	//intialize queue size and number of workers
	var nWorkers, qSize int

	flag.IntVar(&nWorkers, "nWorkers", 5, "number of workers")
	flag.IntVar(&qSize, "qSize", 3, "queue size")

	//initialize queue
	q := Queue{
		JobQueue:   make(chan interface{}, qSize),
		KillSignal: make(chan bool),
	}

	//declare basic job function (sums up natural numbers up to a certain random value from [0,10))
	var jobFnTest JobFn = func(job interface{}) {
		limit := job.(int)
		n := 1 + rand.Intn(limit)
		sum := 0
		for i := 1; i <= n; i++ {
			sum += i
		}
		log.Printf("Sum %v\n", sum)
	}

	//start workers
	StartWorkers(nWorkers, &q, jobFnTest)

	//add jobs
	for i := 1; i <= 10; i++ {
		q.EnqueueJob(i)
		log.Printf("Job %v enqueued\n", i)
	}

	//terminate on ctrl C or kill
	//signal.Notify registers the channel(sigChan) to receive notifications corresponding to the
	//interupt or kill signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	//this line blocks until a value can be retrieved from the sigChan channel
	sig := <-sigChan

	log.Printf("Received %v, exiting", sig)

	//close the killsignal channel to indicate that all workers should be killed (worker goroutines should return)
	KillWorkers(&q)
	//wait until all workers are done
	q.Wg.Wait()
}
