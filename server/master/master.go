package master

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"sync"
	"time"

	pb "github.com/gmarseglia/SDCC-Common/proto"

	"google.golang.org/grpc"
)

var (
	MasterPort        = flag.String("MasterPort", "", "The port for master service.")
	WorkerInfoMap     = make(map[string]WorkerInfo)
	WorkerInfoMapLock sync.RWMutex
	workerChannel     = make(chan int, 1000)
	s                 *grpc.Server
)

type MasterServer struct {
	pb.UnimplementedMasterServer
}

type WorkerInfo struct {
	WorkerAddress string
	QueueSize     int32
	UsageCPU      float32
	Cost          float32
	LastPing      time.Time
}

func getCPUMetric(gotCPU float32) float32 {
	if gotCPU > 0 {
		return gotCPU
	}

	// Get avg CPU usage
	var sum float32 = 0
	found := 0
	for _, info := range WorkerInfoMap {
		if info.UsageCPU > 0 {
			sum += info.UsageCPU
			found++
		}
	}
	if found == 0 {
		return 1
	}
	return sum / float32(found)
}

// NotifyPing implementation
func (s *MasterServer) NotifyPing(ctx context.Context, in *pb.PingRequest) (*pb.PingReply, error) {

	switch in.Type {
	case pb.PingType_ACTIVATE:
		log.Printf("[Master]: Notification from %s of type %s", in.GetWorkerAddress(), in.GetType())
		// Add worker
		_, exists := WorkerInfoMap[in.GetWorkerAddress()]
		if exists {
			return &pb.PingReply{Result: "ALREADY ADDED"}, nil
		} else {
			WorkerInfoMapLock.Lock()
			WorkerInfoMap[in.GetWorkerAddress()] = WorkerInfo{
				WorkerAddress: in.GetWorkerAddress(),
				Cost:          float32(math.Inf(1)),
				LastPing:      time.Now(),
			}
			workerChannel <- len(WorkerInfoMap)
			WorkerInfoMapLock.Unlock()
			log.Printf("[Master]: Added worker %s", in.GetWorkerAddress())
			return &pb.PingReply{Result: "OK"}, nil
		}

	case pb.PingType_PING:
		// Update worker info
		cost := getCPUMetric(in.GetUsageCPU()) * (float32(in.QueueSize) + 1)
		WorkerInfoMapLock.Lock()
		winfo := WorkerInfoMap[in.GetWorkerAddress()]
		winfo.WorkerAddress = in.GetWorkerAddress()
		winfo.QueueSize = in.GetQueueSize()
		winfo.UsageCPU = in.GetUsageCPU()
		winfo.Cost = cost
		winfo.LastPing = time.Now()
		WorkerInfoMap[in.GetWorkerAddress()] = winfo
		WorkerInfoMapLock.Unlock()
		return &pb.PingReply{Result: "PING OK"}, nil

	case pb.PingType_DEACTIVATE:
		log.Printf("[Master]: Notification from %s of type %s", in.GetWorkerAddress(), in.GetType())
		RemoveWorker(in.GetWorkerAddress(), "terminate")
		return &pb.PingReply{Result: "PING OK"}, nil

	default:
		return &pb.PingReply{Result: "UNKNOWN TYPE"}, nil
	}
}

func RemoveWorker(targetWorkerAddr string, cause string) {
	// Check if worker exists
	WorkerInfoMapLock.RLock()
	_, exists := WorkerInfoMap[targetWorkerAddr]
	WorkerInfoMapLock.RUnlock()

	// delete worker address
	if exists {
		WorkerInfoMapLock.Lock()
		delete(WorkerInfoMap, targetWorkerAddr)
		workerChannel <- len(WorkerInfoMap)
		log.Printf("[Master]: Deleted %s due to %s", targetWorkerAddr, cause)
		WorkerInfoMapLock.Unlock()
	}
}

func GetWorkersLen() int {
	WorkerInfoMapLock.RLock()
	defer WorkerInfoMapLock.RUnlock()
	return len(WorkerInfoMap)
}

func checkIn(target WorkerInfo, avoidList []WorkerInfo) bool {
	if avoidList == nil {
		return false
	}
	for _, info := range avoidList {
		if target.WorkerAddress == info.WorkerAddress {
			return true
		}
	}
	return false
}

func GetWorkers(number int, avoidList []WorkerInfo) []WorkerInfo {
	log.Printf("[Master]: Getting %d workers, avoid list len: %d", number, len(avoidList))
	for avoid := range avoidList {
		log.Printf("[Master]: Avoiding %s", avoidList[avoid].WorkerAddress)
	}

	var cheapestWorkerInfo = make([]WorkerInfo, number)
	for i := 0; i < number; i++ {
		cheapestWorkerInfo[i] = WorkerInfo{
			WorkerAddress: "N/A",
			Cost:          float32(math.Inf(1))}
	}

	// Find the cheapest workers
	found := 0
	skipAvoidList := false
	for found < number {
		stepFound := 0
		WorkerInfoMapLock.RLock()
		availableWorkers := len(WorkerInfoMap)
		for _, info := range WorkerInfoMap {
			log.Printf("[Master]: Worker %s cost: %.2f, skipAvoidList: %t, checkIn: %t", info.WorkerAddress, info.Cost, skipAvoidList, checkIn(info, avoidList))
			for i := found; i < number; i++ {
				if info.Cost <= cheapestWorkerInfo[i].Cost && (skipAvoidList || !checkIn(info, avoidList)) {
					log.Printf("[Master]: Worker %s chosen for place %d", info.WorkerAddress, i)
					stepFound++
					// Translate the array
					copy(cheapestWorkerInfo[i+1:], cheapestWorkerInfo[i:number-1])
					cheapestWorkerInfo[i] = info
					i = number // break inner loop
					for k, wi := range cheapestWorkerInfo {
						log.Printf("[Master]: Cost of worker[%d] %s: %.2f", k, wi.WorkerAddress, wi.Cost)
					}
				}
			}
		}
		WorkerInfoMapLock.RUnlock()
		if stepFound == 0 {
			log.Printf("[Master]: No more workers found")
			skipAvoidList = true
		}
		found += stepFound
		if availableWorkers == 0 {
			// Wait for workers to be added
			time.Sleep(time.Second * 1)
		}
	}

	WorkerInfoMapLock.Lock()
	// Increase the cost to avoid overuse on burst traffic
	log.Printf("[Master]: Chosen %d workers:", number)
	for i := 0; i < number; i++ {
		log.Printf("[Master]: Cost of worker[%d] %s: %.2f", i, cheapestWorkerInfo[i].WorkerAddress, cheapestWorkerInfo[i].Cost)
		winfo := cheapestWorkerInfo[i]
		winfo.Cost = float32(winfo.QueueSize+1) * (winfo.UsageCPU * 1.5)
		WorkerInfoMap[cheapestWorkerInfo[i].WorkerAddress] = winfo
		cheapestWorkerInfo[i] = winfo
	}
	WorkerInfoMapLock.Unlock()

	return cheapestWorkerInfo
}

func monitorWorker() {
	go func() {
		for workers := range workerChannel {
			log.Printf("[Master]: Active workers: %d", workers)
		}
	}()

	go func() {
		for {
			// #TODO: set these as env variables
			time.Sleep(time.Second * 3)
			WorkerInfoMapLock.RLock()
			for workerAddr, info := range WorkerInfoMap {
				if time.Since(info.LastPing) > time.Second*3 {
					WorkerInfoMapLock.RUnlock()
					RemoveWorker(workerAddr, "ping timeout")
					WorkerInfoMapLock.RLock()
				}
			}
			WorkerInfoMapLock.RUnlock()
		}
	}()
}

func StartServer() {
	// listen to request to specified port
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", *MasterPort))
	if err != nil {
		log.Fatalf("[Master]: Failed to listen: %v", err)
	}

	// create a new server
	s = grpc.NewServer()

	// register the server
	pb.RegisterMasterServer(s, &MasterServer{})
	log.Printf("[Master]: Listening at %v", lis.Addr())

	workerChannel <- 0
	go monitorWorker()

	// serve the request
	if err := s.Serve(lis); err != nil {
		log.Fatalf("[Master]: Failed to serve: %v", err)
	}
}

func StopServer(wg *sync.WaitGroup) {
	log.Printf("[Master]: Grafecully stopping...")

	// Graceful stop
	s.GracefulStop()
	log.Printf("[Master]: Done.")

	// Comunicate on channel so sync
	(*wg).Done()
}
