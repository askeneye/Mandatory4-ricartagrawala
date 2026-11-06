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
	WANTED   State = "WANTED"
	HELD     State = "HELD"
)

type DeferredRequest struct {
	FromID    string
	Timestamp int64
}

type Node struct {
	pb.UnimplementedRAServiceServer

	mu       sync.Mutex
	nodeId   string
	nodePort string
	peers    map[string]pb.RAServiceClient // Map of peer node IDs to their gRPC clients
	clock    LamportClock
	state    State // Current state of the node (RELEASED, WANTED, HELD)

	requestTs int64 // Timestamp of the current request

	deferred []DeferredRequest // Slice to hold deferred requests

	replies     map[string]bool // Map to track which nodes have replied
	replySignal chan struct{}   // Channel to signal when a reply is received
}

// Manage Ricart Agrawala logic

func (n *Node) RequestCS(ctx context.Context, req *pb.Request) (*pb.Empty, error) {
	n.mu.Lock()

	now := n.clock.Receive(req.Timestamp)

	log.Printf("[%d] Node %s: request from %s with timestamp %d",
		now, n.nodeId, req.FromId, req.Timestamp)

	priority := n.requestTs < req.Timestamp ||
		(n.requestTs == req.Timestamp && n.nodeId < req.FromId)

	log.Printf(
		"[%d] Node %s: REQUEST from %s (ts=%d), state=%s, ownTs=%d, ownPriority=%t",
		now,
		n.nodeId,
		req.FromId,
		req.Timestamp,
		n.state,
		n.requestTs,
		priority,
	)

	if n.state == HELD || (n.state == WANTED && priority) {

		n.deferred = append(n.deferred, DeferredRequest{ // Store the deferred request
			FromID:    req.FromId,
			Timestamp: req.Timestamp,
		})

		log.Printf("[%d] Node %s: deferring reply to %s",
			now, n.nodeId, req.FromId)

		n.mu.Unlock()
		return &pb.Empty{}, nil

	}

	log.Printf(
		"[%d] Node %s: granting permission to %s for request %d",
		now, n.nodeId, req.FromId, req.Timestamp,
	)

	n.mu.Unlock()

	go n.reply(req.FromId, req.Timestamp)

	return &pb.Empty{}, nil
}

func (n *Node) reply(to string, requestTs int64) {

	ts := n.clock.Tick()

	client, exists := n.peers[to]
	if !exists {
		log.Printf("Node %s: unknown peer %s", n.nodeId, to)
		return
	}

	_, err := client.SendReply(
		context.Background(),
		&pb.Reply{
			Timestamp:        ts,
			FromId:           n.nodeId,
			ToId:             to,
			RequestTimestamp: requestTs,
		},
	)

	if err != nil {
		log.Printf("Node %s: reply to %s failed: %v",
			n.nodeId, to, err)
	}
}

func (n *Node) SendReply(ctx context.Context, rep *pb.Reply) (*pb.Empty, error) {

	n.mu.Lock()
	defer n.mu.Unlock()

	now := n.clock.Receive(rep.Timestamp)

	// Check that the reply is intended for this node
	if rep.ToId != n.nodeId {
		return &pb.Empty{}, nil
	}

	// Check that the sender is a known peer
	if _, exists := n.peers[rep.FromId]; !exists {
		return &pb.Empty{}, nil
	}

	// Only accept replies while requesting access
	if n.state != WANTED {
		return &pb.Empty{}, nil
	}

	// Ignore replies to older requests
	// This is the crucial check to ensure the reply corresponds to the current request
	if rep.RequestTimestamp != n.requestTs {
		return &pb.Empty{}, nil
	}

	// Ignore duplicate replies
	if n.replies[rep.FromId] {
		return &pb.Empty{}, nil
	}

	n.replies[rep.FromId] = true

	log.Printf("[%d] Node %s: received reply from %s (%d/%d)",
		now, n.nodeId, rep.FromId,
		len(n.replies), len(n.peers))

	// Notify the waiting requestAccess() method.
	select {
	case n.replySignal <- struct{}{}:
	default:
	}

	return &pb.Empty{}, nil
}

func (n *Node) requestAccess() {

	n.mu.Lock()

	n.state = WANTED
	n.requestTs = n.clock.Tick()
	n.replies = make(map[string]bool)

	ts := n.requestTs

	n.mu.Unlock()

	log.Printf("[%d] Node %s: state WANTED, requesting CS",
		ts, n.nodeId)

	var wg sync.WaitGroup

	for id, client := range n.peers {
		wg.Add(1)

		go func(id string, client pb.RAServiceClient) {
			defer wg.Done()

			_, err := client.RequestCS(
				context.Background(),
				&pb.Request{
					Timestamp: ts,
					FromId:    n.nodeId,
				},
				grpc.WaitForReady(true),
			)

			if err != nil {
				log.Printf("Node %s: request to %s failed: %v",
					n.nodeId, id, err)
			}
		}(id, client)
	}

	wg.Wait()

	// Wait for permission from every peer.
	for {
		n.mu.Lock()

		if len(n.replies) == len(n.peers) {
			n.state = HELD
			n.mu.Unlock()
			break
		}

		n.mu.Unlock()

		<-n.replySignal
	}
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

	n.mu.Unlock()

	log.Printf("Node %s: state RELEASED, sending deferred replies",
		n.nodeId)

	for _, req := range toReply {
		n.reply(req.FromID, req.Timestamp)
	}
}

func main() {
	myId := os.Args[1]

	allNodes := map[string]string{
		"1": "localhost:5051",
		"2": "localhost:5052",
		"3": "localhost:5053",
		"4": "localhost:5054",
	}

	myAdrr, ok := allNodes[myId]
	if !ok {
		log.Fatalf("Unknown node id %s please choose between 1, 2, 3 and 4", myId)
	}

	n := &Node{
		nodeId:      myId,
		nodePort:    myAdrr,
		peers:       make(map[string]pb.RAServiceClient),
		state:       RELEASED,
		replies:     make(map[string]bool),
		replySignal: make(chan struct{}, 1),
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

	lis, err := net.Listen("tcp", myAdrr)
	if err != nil {
		log.Fatalf("Node %s: failed to listen on %s: %v", myId, myAdrr, err)
	}
	s := grpc.NewServer()
	pb.RegisterRAServiceServer(s, n)

	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Node %s: server stopped: %v", myId, err)
		}
	}()

	log.Printf("Node %s: listening on %s with %d peers", myId, myAdrr, len(n.peers))

	for {
		time.Sleep(time.Second * 5)
		n.requestAccess()
		n.criticalSection()
		n.releaseAccess()
	}
}
