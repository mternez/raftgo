package main

import (
	"RaftGo/raft"
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type RequestBody struct {
	Key   string
	Value string
}

func handleSet(raftServer *raft.Server) func(w http.ResponseWriter, req *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		reqBody, unmarshalErr := unmarshalRequestBody(req)
		if unmarshalErr != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write(nil)
		}
		command := KeyValueDbStateMachineCommand{
			CommandType: SET,
			Key:         reqBody.Key,
			Value:       reqBody.Value,
		}
		raftServer.SubmitCommand(marshalCommand(&command))
		commitedCommand := <-raftServer.CommittedCommandsChannel
		result, commandErr := unmarshalResult(&commitedCommand)
		if commandErr != nil {
			http.Error(w, fmt.Sprintf("commited command has an error %s", commitedCommand.Error), 500)
		} else {
			fmt.Fprintf(w, "Result: %s", result)
		}
	}
}

func handleGet(raftServer *raft.Server) func(w http.ResponseWriter, req *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		reqBody, unmarshalErr := unmarshalRequestBody(req)
		if unmarshalErr != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write(nil)
		}
		command := KeyValueDbStateMachineCommand{
			CommandType: GET,
			Key:         reqBody.Key,
			Value:       reqBody.Value,
		}
		raftServer.SubmitCommand(marshalCommand(&command))
		commitedCommand := <-raftServer.CommittedCommandsChannel
		result, commandErr := unmarshalResult(&commitedCommand)
		if commandErr != nil {
			http.Error(w, fmt.Sprintf("commited command has an error %s", commitedCommand.Error), 500)
		} else {
			fmt.Fprintf(w, "Result: %s", result)
		}
	}
}

func handleUnset(raftServer *raft.Server) func(w http.ResponseWriter, req *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		reqBody, unmarshalErr := unmarshalRequestBody(req)
		if unmarshalErr != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write(nil)
		}
		command := KeyValueDbStateMachineCommand{
			CommandType: UNSET,
			Key:         reqBody.Key,
			Value:       reqBody.Value,
		}
		raftServer.SubmitCommand(marshalCommand(&command))
		commitedCommand := <-raftServer.CommittedCommandsChannel
		result, commandErr := unmarshalResult(&commitedCommand)
		if commandErr != nil {
			http.Error(w, fmt.Sprintf("commited command has an error %s", commitedCommand.Error), 500)
		} else {
			fmt.Fprintf(w, "Result: %s", result)
		}
	}
}

func handleGetCurrentStateMachine(stateMachine *KeyValueDbStateMachine) func(w http.ResponseWriter, req *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		fmt.Fprintf(w, "%s", createKeyValuePairs(stateMachine.store))
	}
}

func createKeyValuePairs(m map[string]string) string {
	b := new(bytes.Buffer)
	for key, value := range m {
		fmt.Fprintf(b, "%s=\"%s\"\n", key, value)
	}
	return b.String()
}

func unmarshalRequestBody(req *http.Request) (*RequestBody, error) {
	rawRequestBody, _ := io.ReadAll(req.Body)
	var reqBody RequestBody
	unmarshalErr := json.Unmarshal(rawRequestBody, &reqBody)
	return &reqBody, unmarshalErr
}

func marshalCommand(command *KeyValueDbStateMachineCommand) []byte {
	var marshalledCommand bytes.Buffer
	encoder := gob.NewEncoder(&marshalledCommand)
	encoder.Encode(command)
	return marshalledCommand.Bytes()
}

func unmarshalResult(commitedCommand *raft.CommittedEntry) (*KeyValueDbStateMachineCommandResult, error) {
	var result KeyValueDbStateMachineCommandResult
	decoder := gob.NewDecoder(bytes.NewBuffer(commitedCommand.Result))
	decoder.Decode(&result)
	return &result, commitedCommand.Error
}

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

	stateMachine := NewKeyValueDbStateMachine()

	raftServer := raft.NewServer(
		cfg.Node.Id,
		peers,
		raft.WithHeartbeatTimeout(time.Duration(cfg.MinHeartbeatTimeout), time.Duration(cfg.MaxHeartbeatTimeout)),
		raft.WithElectionTimeout(time.Duration(cfg.ElectionTimeout)),
		raft.WithProbePeersTimeout(time.Duration(cfg.ProbePeersTimeout)),
		raft.WithLogger(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))),
		raft.WithStorage(NewJsonStore(fmt.Sprintf("store_%d.json", cfg.Node.Id))),
		raft.WithStateMachine(stateMachine),
	)

	// Set up RPC
	rpc.Register(raftServer)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%s", cfg.Node.Host, cfg.Node.Port))
	if err != nil {
		log.Fatal("listen error: ", err)
	}

	go raftServer.Serve()

	mux := http.NewServeMux()

	mux.HandleFunc("POST /set/", handleSet(raftServer))
	mux.HandleFunc("GET /get/", handleGet(raftServer))
	mux.HandleFunc("POST /unset/", handleUnset(raftServer))
	mux.HandleFunc("GET /all", handleGetCurrentStateMachine(stateMachine))

	go http.Serve(l, nil)

	http.ListenAndServe(fmt.Sprintf(":%s", cfg.HttpPort), mux)

	fmt.Println(cfg)
}
