package main

import (
	pb "Mandatory4-ricartagrawala/proto"
	"context"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NODE

// Node structure that will implement the RAService defined in the proto
// Node should have an ID, a port, a map of peers, and a Lamport clock

// Node also needs to keep track of its state for the Ricart-Agrawala algorithm
// It should have:
// 1. Mutex to protect its state
// 2. A variable to indicate if it's requesting the CS or currently in the CS
// 3. A timestamp for its request
// 4. Some logic to keep track of how many REPLYs it has received and from whom

type State string
const (
	RELEASED State = "RELEASED"
	WANTED State = "WANTED"
	HELD State = "HELD"
)

type Node struct {
	pb.UnimplementedRAServiceServer

	mu 			sync.Mutex
	nodeId 		string
	nodePort 	string
	peers 		map[string]pb.RAServiceClient
	clock 		LamportClock
	state 		State

	requestTs 	int64
	deferred 	[]string
	replies		chan struct{}
}

// Manage Ricart Agrawala logic

func (n *Node) RequestCS(ctx context.Context, req *pb.Request) (*pb.Empty, error) {
	n.mu.Lock()
	
	now := n.clock.Receive(req.Timestamp)
	log.Printf("[%d] Node %s: request from %s with timestamp %d", now , n.nodeId, req.FromId, req.Timestamp)

	priority := n.requestTs < req.Timestamp || (n.requestTs == req.Timestamp && n.nodeId < req.FromId)

	if n.state == HELD || (n.state == WANTED && priority) {
		n.deferred = append(n.deferred, req.FromId) //Reply to later
		log.Printf("[%d] Node %s: deferring reply to %s", now, n.nodeId, req.FromId)
		n.mu.Unlock()
		return &pb.Empty{}, nil
	}

	ts := n.clock.Tick()	
	n.mu.Unlock()

	go n.reply(req.FromId, ts)
	return &pb.Empty{}, nil
}

func (n *Node) reply(to string, ts int64) {
	_, err := n.peers[to].SendReply(context.Background(), &pb.Reply{Timestamp: ts, FromId: n.nodeId})
	if err != nil {
		log.Printf("Node %s: reply to %s failed: %v", n.nodeId, to, err)
	}
}

func (n *Node) SendReply(ctx context.Context, rep *pb.Reply) (*pb.Empty, error) {

	now := n.clock.Receive(rep.Timestamp)
	log.Printf("[%d] Node %s: got reply from %s", now, n.nodeId, rep.FromId)

	n.replies <- struct{}{}
	return &pb.Empty{}, nil
}

func (n *Node) requestAccess() {
	n.mu.Lock()
	n.state = WANTED
	n.requestTs = n.clock.Tick()
	ts := n.requestTs
	n.mu.Unlock()

	log.Printf("[%d] Node %s: state WANTED, sending REQUEST (ts=%d) to all peers", ts, n.nodeId, ts)

	for id, client := range n.peers {
		_, err := client.RequestCS(context.Background(), 
		&pb.Request{Timestamp: ts, FromId: n.nodeId},
		grpc.WaitForReady(true))
		if err != nil {
			log.Printf("Node %s: request to %s failed %v", n.nodeId, id, err)
		}
	}

	for i := 0; i < len(n.peers); i++ {
		<-n.replies
	}

	n.mu.Lock()
	n.state = HELD
	n.mu.Unlock()
}

func (n *Node) criticalSection() {
	log.Printf("[%d] Node %s: state HELD, >>> ENTER critical section", n.clock.Read(), n.nodeId)
	time.Sleep(1 * time.Second)
	log.Printf("[%d] Node %s: <<< EXIT critical section", n.clock.Read(), n.nodeId)
}

func (n *Node) releaseAccess() {
	n.mu.Lock()
	n.state = RELEASED
	toReply := n.deferred
	n.deferred = nil
	ts := n.clock.Tick()
	n.mu.Unlock()

	log.Printf("[%d] Node %s: state RELEASED, sending deferred replies to %v", ts, n.nodeId, toReply)

	for _, id := range toReply {
		n.reply(id, ts)
	}
}

func main() {
	myId := os.Args[1]
	
	allNodes := map[string]string{
		"1" : "localthost:5051",
		"2" : "localthost:5052",
		"3" : "localthost:5053",
		"4" : "localthost:5054",
	}

	myAdrr, ok := allNodes[myId]
	if !ok {
		log.Fatalf("Unknown node id %s please choose between 1, 2, 3 and 4", myId)
	}

	n := &Node{
		nodeId: myId,
		nodePort: myAdrr,
		peers: make(map[string]pb.RAServiceClient),
		state: RELEASED,
	}

	//Creating rest of clients (LAZY: they don't need to be up yet)
	for id, addr := range allNodes {
		if id == myId {
			continue
		}
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("Node %s: could not create client for %s: %v", myId, id, err)
		}
		n.peers[id] = pb.NewRAServiceClient(conn)
	} 
	n.replies = make(chan struct{}, len(n.peers))

	lis, err := net.Listen("tcp", myAdrr)
	if err != nil {
		log.Fatalf("Node %s: failed to listen on %s: %v", myId, myAdrr, err)
	}
	s := grpc.NewServer()
	pb.RegisterRAServiceServer(s, n)

	go func() {
		if err := s.Serve(lis)
		err != nil {
			log.Fatalf("Node %s: server stopped: %v", myId, err)
		}
	}()

	log.Printf("Node %s: listening on %s with %d peers", myId, myAdrr, len(n.peers))

	for {
		time.Sleep(time.Second * 2)
		n.requestAccess()
		n.criticalSection()
		n.releaseAccess()
	}
}
