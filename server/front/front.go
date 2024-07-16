package front

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"server/master"
	"sort"
	"sync"
	"time"

	pb "github.com/gmarseglia/SDCC-Common/proto"
	"github.com/gmarseglia/SDCC-Common/utils"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var (
	FrontPort        = flag.String("FrontPort", "", "The port for front service.")
	ParallelWorkers  = flag.Int("ParallelWorkers", -1, "The number of parallel workers.")
	ReplicationGrade = flag.Int("ReplicationGrade", -1, "The replication grade.")
	counter          int32
	counterLock      sync.Mutex
	s                *grpc.Server
	RequestTimeout   = time.Minute * 5
)

// FrontServer is used to implement the MiniServer interface
type FrontServer struct {
	pb.UnimplementedFrontServer
}

type ActiveWorker struct {
	ID      int
	Info    master.WorkerInfo
	Active  bool
	Ready   bool
	Client  *pb.BackClient
	Request *pb.ConvolutionalLayerBackRequest
}

func assignIndexes(parallelWorkers int, replicationGrade int, index int) []int {
	result := make([]int, replicationGrade)
	for i := 0; i < replicationGrade; i++ {
		result[i] = (index + i) % parallelWorkers
	}
	sort.Ints(result)
	return result
}

func checkCompleted(blocks []int, completedBlocks map[int]bool, lock *sync.RWMutex) bool {
	lock.Lock()
	defer lock.Unlock()
	for _, block := range blocks {
		if !completedBlocks[block] {
			return false
		}
	}
	return true
}

func setReady(workersMapsLock *sync.RWMutex, workersMap map[int]ActiveWorker, i int, ready bool) {
	workersMapsLock.RLock()
	entry := workersMap[i]
	workersMapsLock.RUnlock()
	entry.Ready = ready
	workersMapsLock.Lock()
	workersMap[i] = entry
	workersMapsLock.Unlock()
}

func readWorkersMap(workersMapsLock *sync.RWMutex, workersMap map[int]ActiveWorker, i int) ActiveWorker {
	workersMapsLock.RLock()
	aw := workersMap[i]
	workersMapsLock.RUnlock()
	return aw
}

func writeWorkersMap(workersMapsLock *sync.RWMutex, workersMap map[int]ActiveWorker, i int, entry ActiveWorker) {
	workersMapsLock.Lock()
	workersMap[i] = entry
	workersMapsLock.Unlock()
}

func (s *FrontServer) ConvolutionalLayer(ctx context.Context, in *pb.ConvolutionalLayerFrontRequest) (*pb.ConvolutionalLayerFrontReply, error) {
	// generate Request ID
	counterLock.Lock()
	counter += 1
	var id int32 = counter
	counterLock.Unlock()

	// Compute parallelization parameters
	var actualParallelWorkers int
	var actualReplicationGrade int
	if in.GetUseKernels() {
		actualParallelWorkers = max(min(*ParallelWorkers, master.GetWorkersLen(), len(in.Kernel)), 1)
	} else {
		actualParallelWorkers = 1
	}
	actualReplicationGrade = min(*ReplicationGrade, actualParallelWorkers)

	log.Printf("[Front]: Request #%d started. Parallel workers: %d, Replication grade: %d", id, actualParallelWorkers, actualReplicationGrade)

	// Split the kernels into blocks
	kernelsMap := make(map[int][]*pb.Matrix)
	var kernelBlockSize int = int(math.Floor(float64(len(in.Kernel)) / float64(actualParallelWorkers)))
	for i := 0; i < actualParallelWorkers-1; i++ {
		kernelsMap[i] = in.Kernel[i*kernelBlockSize : min((i+1)*kernelBlockSize, len(in.Kernel))]
	}
	kernelsMap[actualParallelWorkers-1] = in.Kernel[(actualParallelWorkers-1)*kernelBlockSize : len(in.Kernel)]

	// Join blocks into splits using the indexes
	indexesMap := make(map[int][]int)
	backRequestMap := make(map[int]*pb.ConvolutionalLayerBackRequest)
	for i := 0; i < actualParallelWorkers; i++ {
		// Compute blocks indexes
		indexesMap[i] = assignIndexes(actualParallelWorkers, actualReplicationGrade, i)
		splitBlocks := indexesMap[i]

		// Append all kernels to produce the list
		var split []*pb.Matrix
		for j := 0; j < len(splitBlocks); j++ {
			block := kernelsMap[splitBlocks[j]]
			split = append(split, block...)
		}

		// Create back request
		backRequestMap[i] = &pb.ConvolutionalLayerBackRequest{
			Target:      in.GetTarget(),
			Kernel:      split,
			AvgPoolSize: in.GetAvgPoolSize(),
			UseKernels:  in.GetUseKernels(),
			UseSigmoid:  in.GetUseSigmoid(),
			ID:          id,
			InnerID:     int32(i)}
	}

	// Get first n workers
	workersMapsLock := sync.RWMutex{}
	workers := master.GetWorkers(actualParallelWorkers, nil)
	workersMap := make(map[int]ActiveWorker)
	for i := 0; i < actualParallelWorkers; i++ {
		workersMap[i] = ActiveWorker{
			ID:     i,
			Info:   workers[i],
			Active: false,
			Ready:  true,
		}
	}

	// time the call
	startTime := time.Now()

	// Initialize channels
	successChan := make(chan int, actualParallelWorkers)
	replaceChan := make(chan int)
	fatalChannel := make(chan error)
	readyChan := make([]chan bool, actualParallelWorkers)
	for i := 0; i < actualParallelWorkers; i++ {
		readyChan[i] = make(chan bool)
	}
	finalChan := make(chan error)

	// Initialize completedBlocks and completedResult
	completeBlocksLock := sync.RWMutex{}
	completedBlocks := make(map[int]bool, actualParallelWorkers)
	for i := 0; i < actualParallelWorkers; i++ {
		completedBlocks[i] = false
	}
	completedResult := make([]*pb.Matrix, len(in.Kernel))

	wg := &sync.WaitGroup{}
	wg.Add(actualParallelWorkers)

	// Once for request, launch a gouroutine that will manage the worker go routines
	go func() {
		for {
			select {
			case innerID := <-successChan:
				log.Printf("[Front]: Completed %d.%d", id, innerID)
				// Check if all completed
				allCompleted := true
				completeBlocksLock.RLock()
				for i := 0; i < actualParallelWorkers; i++ {
					if !completedBlocks[i] {
						allCompleted = false
						i = actualParallelWorkers // break inner for
					}
				}
				completeBlocksLock.RUnlock()

				if allCompleted {
					// Cancel pending requests
					finalChan <- nil
					cancelAllActiveWorkers(&workersMapsLock, workersMap, id)
					log.Printf("[Front]: Tearing down %d due to success", id)
					wg.Wait()
					log.Printf("[Front]: Teared down %d", id)
					return
				}

			case replaceIndex := <-replaceChan:
				log.Printf("[Front]: Replacing %d.%d", id, replaceIndex)
				newWorker := master.GetWorkers(1, workers)
				aw := readWorkersMap(&workersMapsLock, workersMap, replaceIndex)
				aw.Info = newWorker[0]
				aw.Ready = true
				writeWorkersMap(&workersMapsLock, workersMap, replaceIndex, aw)
				log.Printf("[Front]: Replaced %d.%d with %s", id, replaceIndex, newWorker[0].WorkerAddress)
				readyChan[replaceIndex] <- true

			case err := <-fatalChannel:
				// Cancel pending requests
				finalChan <- err
				cancelAllActiveWorkers(&workersMapsLock, workersMap, id)
				log.Printf("[Front]: Tearing down %d due to fatal failure", id)
				wg.Wait()
				log.Printf("[Front]: Teared down %d", id)
				return
			}
		}
	}()

	// For each part launch a goroutine that will handle comunication with the worker
	for i := 0; i < actualParallelWorkers; i++ {
		go func(i int) {
			// Call RPC to worker
			for {
				// Get active worker
				aw := readWorkersMap(&workersMapsLock, workersMap, i)
				ready := aw.Ready

				// Wait for ready, in case of replacing
				if !ready {
					log.Printf("[Front]: Waiting for replace %d.%d", id, i)
					<-readyChan[i]
				}

				// Check if the request has been completed
				if checkCompleted(indexesMap[i], completedBlocks, &completeBlocksLock) {
					log.Printf("[Front]: Exiting due to completion %d.%d", id, i)
					wg.Done()
					return
				}

				// Acquire worker address
				aw = readWorkersMap(&workersMapsLock, workersMap, i)
				addr := aw.Info.WorkerAddress

				log.Printf("[Front]: Sending %d.%d len(%v)=%d  to %s", id, i, indexesMap[i], len(backRequestMap[i].Kernel), addr)

				// Set up a connection to the gRPC server
				conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					log.Printf("[Front]: %s did not connect: %v", addr, err)
					master.RemoveWorker(addr, "not connected")
					setReady(&workersMapsLock, workersMap, i, false)
					replaceChan <- i
					continue
				}
				defer conn.Close()

				// create the client object
				c := pb.NewBackClient(conn)

				// Write client to worker map to be used to cancel
				aw = readWorkersMap(&workersMapsLock, workersMap, i)
				aw.Client = &c
				writeWorkersMap(&workersMapsLock, workersMap, i, aw)

				// create the context
				ctxInternal, cancel := context.WithTimeout(context.Background(), RequestTimeout)
				defer cancel()

				// Set workers as active
				aw = readWorkersMap(&workersMapsLock, workersMap, i)
				aw.Active = true
				writeWorkersMap(&workersMapsLock, workersMap, i, aw)

				// contact the server
				WorkerResponse, err := c.ConvolutionalLayer(ctxInternal, backRequestMap[i])

				// Set workers as inactive
				aw = readWorkersMap(&workersMapsLock, workersMap, i)
				aw.Active = false
				writeWorkersMap(&workersMapsLock, workersMap, i, aw)

				// Check if error
				if err != nil {
					// Check if reply is not avaible due to size too big
					s, ok := status.FromError(err)
					if ok && s.Code() == codes.ResourceExhausted {
						log.Printf("[Front]: Response: %d.%d: Resource exhausted error occurred: %s by %s.", id, i, s.Message(), addr)
						wg.Done()
						fatalChannel <- err
						return
					} else if ok && s.Code() == codes.Canceled {
						log.Printf("[Front]: Cancelled %d.%d (%v) to %s", id, i, indexesMap[i], addr)
						wg.Done()
						return
					} else if ok && s.Code() == codes.Unavailable {
						log.Printf("[Front]: %s is unreachable: %v", addr, err)
						master.RemoveWorker(addr, "unreachable")
						setReady(&workersMapsLock, workersMap, i, false)
						replaceChan <- i
						continue
					} else {
						log.Printf("[Front]: Response: %d.%d: unkwown error occurred: %s by %s.", id, i, s.Message(), addr)
						wg.Done()
						fatalChannel <- err
						return
					}
				}

				// Set completed
				log.Printf("[Front]: Received %d.%d len(%v)=%d from %s", id, i, indexesMap[i], len(WorkerResponse.GetResult()), addr)
				completeBlocksLock.Lock()
				newCompletedBlocks := indexesMap[i]
				for j := 0; j < len(newCompletedBlocks); j++ {
					completedBlockIndex := newCompletedBlocks[j]
					completedBlocks[completedBlockIndex] = true

					// Write completed kernels to completedResults
					var begin int = completedBlockIndex * kernelBlockSize
					var end int
					if completedBlockIndex == actualParallelWorkers-1 {
						end = len(in.Kernel)
					} else {
						end = begin + kernelBlockSize
					}
					offset := j * kernelBlockSize
					log.Printf("[DEBUG]: Writing %d to %d, for block %d, offset: %d", begin, end, completedBlockIndex, offset)
					for k := begin; k < end; k++ {
						completedResult[k] = WorkerResponse.GetResult()[k-begin+offset]
					}
				}
				completeBlocksLock.Unlock()

				// Notify outer
				wg.Done()
				successChan <- i
				return
			}
		}(i)
	}

	outcoume := <-finalChan
	if outcoume == nil {
		log.Printf("[Front]: Response: %d complete in %d ms", id, time.Since(startTime).Milliseconds())

		// #TODO: remove if working
		for i, protoResult := range completedResult {
			result := utils.ProtoToMatrix(protoResult)
			utils.PrettyPrint(fmt.Sprintf("Result: %d", i), result)
		}

		return &pb.ConvolutionalLayerFrontReply{ID: id, Result: completedResult}, nil
	} else {
		log.Printf("[Front]: Response: %d failed in %d ms", id, time.Since(startTime).Milliseconds())
		return nil, outcoume
	}
}

func cancelAllActiveWorkers(workersMapsLock *sync.RWMutex, workersMap map[int]ActiveWorker, id int32) {
	for i := 0; i < len(workersMap); i++ {
		aw := readWorkersMap(workersMapsLock, workersMap, i)
		if aw.Active {
			go func(i int) {
				// create the context
				ctxInternal, cancel := context.WithTimeout(context.Background(), time.Minute*5)
				defer cancel()
				c := *aw.Client
				c.Cancel(ctxInternal, &pb.CancelRequest{ID: id, InnerID: int32(i)})
			}(i)
		}
	}
}

func StartFrontServer() {
	// listen to request to specified port
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", *FrontPort))
	if err != nil {
		log.Fatalf("[Front]: Failed to listen: %v", err)
	}

	// create a new server
	s = grpc.NewServer()

	// register the server
	pb.RegisterFrontServer(s, &FrontServer{})
	log.Printf("[Front]: Listening at %v", lis.Addr())

	// serve the request
	if err := s.Serve(lis); err != nil {
		log.Fatalf("[Front]: Failed to serve: %v", err)
	}
}

func StopServer(wg *sync.WaitGroup) {
	log.Printf("[Front]: Grafecully stopping...")

	// Graceful stop
	s.GracefulStop()
	log.Printf("[Front]: Done.")

	// Comunicate on channel so sync
	(*wg).Done()
}
