package main

import (
	"RaftGo/raft"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

func main() {

	if len(os.Args) < 2 {
		fmt.Println("Missing cfg.yml")
		return
	}

	cfg := ReadConfiguration(os.Args[1])

	peers := make([]*raft.Peer, len(cfg.Peers))

	for ind, peer := range cfg.Peers {

		peerClient, err := NewRpcClient(peer.Host, peer.Port)

		if err != nil {
			fmt.Printf("Couldn't dial peer %s:%s : %s\n", peer.Host, peer.Port, err)
		}

		peers[ind] = raft.NewPeer(peer.Id, peerClient)
	}

	server := raft.NewServer(
		cfg.Node.Id,
		peers,
		raft.WithHeartbeatTimeout(time.Duration(cfg.MinHeartbeatTimeout), time.Duration(cfg.MaxHeartbeatTimeout)),
		raft.WithElectionTimeout(time.Duration(cfg.ElectionTimeout)),
		raft.WithProbePeersTimeout(time.Duration(cfg.ProbePeersTimeout)),
		raft.WithLogger(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))),
		raft.WithStorage(NewJsonStore(fmt.Sprintf("store_%d.json", cfg.Node.Id))),
		raft.WithStateMachine(NewKeyValueDbStateMachine()),
		raft.WithOnComittedCommand(func(command []byte) { fmt.Println("Log committed") }),
	)

	// Set up RPC
	rpc.Register(server)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%s", cfg.Node.Host, cfg.Node.Port))
	if err != nil {
		log.Fatal("listen error: ", err)
	}

	go server.Serve()

	http.Serve(l, nil)

	fmt.Println(cfg)
}
