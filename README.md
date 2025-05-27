# Overview
This is a personal project that implements a Redis-backed priority task queue (using a sorted set / ZSET) with Go-based workers, timeouts, and backpressure control.

# How to Run 
1. Ensure redis server is running on localhost:6379
2. `go build cmd/task-queue/main.go`
3. `./main --nWorkers 3 --qSize 2` (omitting --nWorkers and --qSize results in using default values, 5 & 3 respectively)

# Customization
1. Replace the job function in main.go with your own job function. Ensure the function is of type JobFn 

# How It Works
1. Startup
    - Define a specific number of workers and the task queue size before starting the program. These can be passed as command line arguments
    - Ensure that Redis is running on localhost:6379
2. Queue Initialization
    - JobQueueControl -> a buffered channel used to control the maximum number of jobs that can exist in the Redis task queue
    - RedisClient -> the client object that allows us to interface with the redis server
    - KillSignal -> A channel used to signal all workers to terminate 
4. Job Function
    - The function implementation can be customized, however it's signature and return type correspond to the JobFn definition 
    - The provided example simply sums up the first n natural numbers where n is configured based on the input (job data) passed to the job function
5. Worker Lifecycle
    - Startup the workers. The workers will wait for either a job or the kill signal
    - When a job is available, the worker will pop the job data from the zset
    - The job data is then deserialized
    - The job function is executed using the job data
    - The result (either the job function’s return value or a timeout) is then logged by the worker
6. Enqueueing Jobs 
    - Add job data to the zset (with a priority score) after serialization 
    - Check if the task queue is already full. This is managed by the JobQueueControl channel; execution blocks and prevents the job from being enqueued until a job already exisiting in the queue is popped and handled by a worker. This simulates backpressure
7. Shutdown Logic
    - Once all jobs are done, the program waits for either new jobs to be added to the zset (say via a separate producer program, not included here), SIGINT (ctrl + C) or SIGTERM
    - If SIGINT or SIGTERM is sent by the user, the kill signal associated with the queue is broadcast to all workers and the workers exit cleanly  
    - The program can also be forcefully terminated using `kill -9 <pid>`

# Tradeoff
A zset was chosen over another redis backed data structure (such as a list) or redis streams (more appropriate for scenarios such as logging) since zset provides an out of the box implementation of a priority queue. It does however, require careful management of scenarios like backpressure (which has been implemented here)

# Potential Improvements
1. Unit Tests: Good to have but omitted for brevity since the intention of this code is to provide a template/guide for a generic task queue implementation
2. Retries: Another good to have but dependent on the specific 'real world' requirements. Accordingly, either the job could be re-enqueued or sent to a dead letter queue
3. Job Result Persistence: Again dependent on real world scenarios, results could be stored in Redis (and then used subsequently for scenarios such as building a dashboard) 