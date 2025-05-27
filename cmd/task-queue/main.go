package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Job struct {
	JobData interface{}
	Id      string
}

type JobFn func(job Job) interface{}

type Worker struct {
	ExecuteJob JobFn
	Id         string
}

type Queue struct {
	//JobQueueControl restricts how many jobs at max can exist
	//in the redis sorted set (ZSET)
	JobQueueControl chan interface{}
	RedisClient     *redis.Client
	KillSignal      chan bool
	Wg              sync.WaitGroup
}

var queueKey string = "task_queue"

func KillWorkers(q *Queue) {
	//ensures that all workers receive the kill signal and stop processing
	close(q.KillSignal)
}

var timeoutDefault int = 30000

func (w *Worker) ProcessJob(q *Queue, parentCtx context.Context) {
	for {
		select {
		//if there is a job in available (determined by checking if a value can be retrieved from the JobQueueControl channel)
		case <-q.JobQueueControl:
			//add another select here with timeout logic
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutDefault)*time.Millisecond)

			//using a buffered channel avoids a goroutine leak
			//assume we use an unbuffered channel
			//imagine a scenario where the timeout is achieved first, so the logic under <- ctx.Done() is executed
			//in this case the goroutine (which is continuing to execute) would reach done <- true and block indefinitely (goroutine leak)
			//since there is no receiver to take the value from done. this would not block if using a buffered channel
			done := make(chan bool, 1)

			//retrieve the job with lowest score from the zset (lowest score impliest highest priority, i.e., score 1 is the highest priority job in this case)
			jobs, err := q.RedisClient.ZPopMin(parentCtx, queueKey, 1).Result()

			//if there is no error and there is a job in the queue, execute the job
			if err == nil && len(jobs) > 0 {
				//jobs[0].Member is of type interface, type assert to string and unmarshal to Job before calling w.ExecuteJob
				jobStr := jobs[0].Member.(string)
				var job Job
				err := json.Unmarshal([]byte(jobStr), &job)

				if err != nil {
					log.Printf("Error unmarshaling job %v\n", err)
				}

				//store the result of job execution
				var result interface{}
				go func() {
					//done receives true even if the executejob panics
					defer func() { done <- true }()
					result = w.ExecuteJob(job)
				}()

				//poll if the job is done or if timeout is achieved first, log a message accordingly
				select {
				case <-done:
					//once the result is ready, type assert to int (since the result is of type int in this case)
					//and then log the result
					res := result.(int)
					log.Printf("Job %v completed by worker %v. Result: %v\n", job.Id, w.Id, res)
				case <-ctx.Done():
					log.Printf("Job %v timed out for worker %v\n", job.Id, w.Id)
				}
			} else {
				log.Printf("Error from worker %v popping job from zset %v\n", w.Id, err)
			}
			cancel()

		//if a value can be retrieved from the KillSignal channel, stop all workers
		case <-q.KillSignal:
			q.Wg.Done()
			return
		}
	}
}

func StartWorkers(numWorkers int, q *Queue, jobFn JobFn, ctx context.Context) {
	q.Wg.Add(numWorkers)
	for i := 1; i <= numWorkers; i++ {
		wId := uuid.New()
		w := Worker{ExecuteJob: jobFn, Id: wId.String()}
		go w.ProcessJob(q, ctx)
		log.Printf("Started worker %v\n", w.Id)
	}
}

func (q *Queue) EnqueueJob(job Job, ctx context.Context, priority int) (int64, error) {
	//blocks when queue size is exceeded
	q.JobQueueControl <- struct{}{}

	//serialize job and add it to the sorted set
	jobSerialized, err := json.Marshal(job)
	if err != nil {
		log.Printf("Error serializing job, aborting enqueueing process %v\n", err)
		return 0, err
	}

	res, err := q.RedisClient.ZAdd(ctx, queueKey, redis.Z{
		Score:  float64(priority),
		Member: jobSerialized,
	}).Result()

	//return error or number of records inserted
	return res, err
}

func main() {
	//intialize queue size and number of workers
	var nWorkers, qSize int

	flag.IntVar(&nWorkers, "nWorkers", 5, "number of workers")
	flag.IntVar(&qSize, "qSize", 3, "queue size")

	//initialize redis client
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx := context.Background()

	//initialize queue
	q := Queue{
		JobQueueControl: make(chan interface{}, qSize),
		RedisClient:     client,
		KillSignal:      make(chan bool),
	}

	//declare basic job function (sums up natural numbers up to a certain random value from [0,10))
	var jobFnTest JobFn = func(job Job) interface{} {
		//type assertion to retrieve the actual job data
		//in this case the job is of type float64
		//note: when encoding/json uses float64 when unmarshalling numbers into interface{} values (refer ProcessJob)
		//hence we type assert to float64 and then type cast to int
		limit := int(job.JobData.(float64))

		n := 1 + rand.Intn(limit)
		sum := 0
		for i := 1; i <= n; i++ {
			sum += i
		}
		return sum
	}

	//start workers
	StartWorkers(nWorkers, &q, jobFnTest, ctx)

	//add jobs
	for i := 1; i <= 10; i++ {
		//job priority will be a random number in the interval [1,qSize]
		priority := 1 + rand.Intn(qSize)

		//generate job id
		jobId := uuid.New()
		//create job
		job := Job{JobData: i, Id: jobId.String()}

		//enqueue job
		_, err := q.EnqueueJob(job, ctx, priority)

		//log if the job was inserted successfully or if there was an error
		if err != nil {
			log.Printf("Error enqueueing job %v: %v\n", job.Id, err)
		} else {
			log.Printf("Job %v enqueued", job.Id)
		}
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
