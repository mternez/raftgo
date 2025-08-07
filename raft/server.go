package raft

import (
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	_tickerUnit                       = time.Millisecond
	_defaultInitialGracePeriodTimeout = 500
	_defaultProbePeersTimeout         = 500
	_defaultMinHeartbeatTimeout       = 150
	_defaultMaxHeartbeatTimeout       = 300
	_defaultElectionTimeout           = 500
)

type ServerNode struct {
	state       NodeState
	id          int // This instance's id in the cluster
	currentTerm int // current term of this instance
	votes       int // Number of votes of this instance has received this election
	votedFor    int // Peer for whom this instance has voted
	Entries     []*LogEntry
	// Volatile
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	// Only for leaders
	nextIndex  map[int]int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex map[int]int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

type ServerConfiguration struct {
	probePeersTimeout   time.Duration
	minHeartbeatTimeout time.Duration
	maxHeartbeatTimeout time.Duration
	electionTimeout     time.Duration
}

type Server struct {
	cluster                  *Cluster
	requestVoteWaitGroup     sync.WaitGroup
	appendEntriesWaitGroup   sync.WaitGroup
	mutex                    sync.Mutex
	electionTicker           *time.Ticker // Ticker for election period
	listenHeartbeatTicker    *time.Ticker // Ticker for leader hearbeat
	sendHeartbeatTicker      *time.Ticker
	configuration            *ServerConfiguration
	node                     *ServerNode
	logger                   Logger
	storage                  ServerNodeStorage
	stateMachine             StateMachine
	CommittedCommandsChannel chan CommittedEntry
}

type CommittedEntry struct {
	Index   int
	Command []byte
	Result  []byte
	Error   error
}

type ServerOption func(*Server)

func WithHeartbeatTimeout(min, max time.Duration) ServerOption {
	return func(s *Server) {
		s.configuration.minHeartbeatTimeout = min
		s.configuration.maxHeartbeatTimeout = max
	}
}

func WithElectionTimeout(timeout time.Duration) ServerOption {
	return func(s *Server) {
		s.configuration.electionTimeout = timeout
	}
}

func WithProbePeersTimeout(timeout time.Duration) ServerOption {
	return func(s *Server) {
		s.configuration.probePeersTimeout = timeout
	}
}

func WithLogger(logger Logger) ServerOption {
	return func(s *Server) {
		s.logger = logger
	}
}

func WithStorage(storage ServerNodeStorage) ServerOption {
	return func(s *Server) {
		s.storage = storage
	}
}

func WithStateMachine(stateMachine StateMachine) ServerOption {
	return func(s *Server) {
		s.stateMachine = stateMachine
	}
}

func WithDefault() ServerOption {
	return func(s *Server) {
		s.logger = slog.New(slog.NewTextHandler(os.Stdout, nil))
		s.configuration.probePeersTimeout = time.Duration(_defaultProbePeersTimeout) * _tickerUnit
		s.configuration.minHeartbeatTimeout = time.Duration(_defaultMinHeartbeatTimeout) * _tickerUnit
		s.configuration.maxHeartbeatTimeout = time.Duration(_defaultMaxHeartbeatTimeout) * _tickerUnit
		s.configuration.electionTimeout = time.Duration(_defaultElectionTimeout) * _tickerUnit
	}
}

func NewServer(nodeId int, peers []*Peer, options ...ServerOption) *Server {
	s := &Server{
		CommittedCommandsChannel: make(chan CommittedEntry, 1),
		mutex:                    sync.Mutex{},
		requestVoteWaitGroup:     sync.WaitGroup{},
		appendEntriesWaitGroup:   sync.WaitGroup{},
		node:                     &ServerNode{id: nodeId, votes: 0, votedFor: _nilPeerId, commitIndex: 0, lastApplied: 0},
		cluster:                  &Cluster{peers: peers, leader: nil},
		configuration:            &ServerConfiguration{},
	}
	// If no options provided, apply default configuration
	if len(options) == 0 {
		WithDefault()(s)
	}
	for _, opt := range options {
		opt(s)
	}
	return s
}

func (s *Server) Serve() {
	s.restorePersistedState()
	s.waitInitialGracePeriod()
	s.probePeers()
	for {
		s.mutex.Lock()
		switch s.node.state {
		case FOLLOWER:
			s.mutex.Unlock()
			s.runFollower()
		case CANDIDATE:
			s.mutex.Unlock()
			s.runCandidate()
		case LEADER:
			s.mutex.Unlock()
			s.runLeader()
		default:
			s.mutex.Unlock()
		}
	}
}

func (s *Server) restorePersistedState() {
	if s.storage != nil {
		persistedState, err := s.storage.Fetch()
		if err != nil {
			panic(err)
		}
		s.node.currentTerm = persistedState.CurrentTerm
		s.node.votedFor = persistedState.VotedFor
		s.node.Entries = make([]*LogEntry, len(persistedState.Entries))
		copy(s.node.Entries, persistedState.Entries)
	}
}

func (s *Server) waitInitialGracePeriod() {
	gracePeriodTicker := time.NewTicker(_defaultInitialGracePeriodTimeout * _tickerUnit)
	select {
	case <-gracePeriodTicker.C:
		gracePeriodTicker.Stop()
		break
	}
}

func (s *Server) probePeers() {
	// Goroutine to regularly reconnect to the peers
	probePeersTicker := time.NewTicker(s.configuration.probePeersTimeout * _tickerUnit)
	go func() {
		for {
			select {
			case <-probePeersTicker.C:
				for _, peer := range s.cluster.peers {
					peer.client.Reconnect()
				}
			}
		}
	}()
}

func (s *Server) RequestVote(payload RequestVotePayload, reply *RequestVoteReply) error {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.logger.Debug("received RequestVote", "node", s.node, "candidate", payload)

	if payload.CandidateTerm < s.node.currentTerm {
		reply.FollowerVoteGranted = false
		reply.FollowerTerm = s.node.currentTerm
		return nil
	}

	if payload.CandidateTerm > s.node.currentTerm {
		s.becomeFollower(payload.CandidateTerm)
	}

	reply.FollowerTerm = s.node.currentTerm

	var candidateLastLogUpToDate bool
	lastLogEntry := s.getLastLog()
	if lastLogEntry != nil {
		candidateLastLogUpToDate = payload.CandidateLastLogTerm > lastLogEntry.Term || (payload.CandidateLastLogTerm == lastLogEntry.Term && payload.CandidateLastLogIndex >= lastLogEntry.Index)
	} else {
		candidateLastLogUpToDate = true
	}

	if s.node.state != LEADER && (s.node.votedFor == _nilPeerId || s.node.votedFor == payload.CandidateId) && candidateLastLogUpToDate {
		s.node.votedFor = payload.CandidateId
		reply.FollowerVoteGranted = true
	} else {
		reply.FollowerVoteGranted = false
	}

	return nil
}

func (s *Server) AppendEntries(payload AppendEntriesPayload, reply *AppendEntriesReply) error {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.logger.Debug("received AppendEntries", "node", s.node, "leader", payload)

	if payload.LeaderTerm < s.node.currentTerm {
		reply.Success = false
		reply.FollowerTerm = s.node.currentTerm
		return nil
	}

	if payload.LeaderTerm > s.node.currentTerm {
		s.becomeFollower(payload.LeaderTerm)
	}

	if len(payload.Entries) != 0 && s.node.state != LEADER {
		prevLogEntry := s.getLogEntryByIndex(payload.PrevLogIndex)
		if prevLogEntry != nil && !(prevLogEntry.Term == payload.PrevLogTerm) {
			reply.Success = false
			reply.FollowerTerm = s.node.currentTerm
			s.updateEntries(payload.Entries)
			return nil
		}
		if payload.LeaderCommitIndex > s.node.commitIndex {
			s.updateCommitIndex(payload.LeaderCommitIndex, payload.Entries)
		}
		s.updateEntries(payload.Entries)
	}

	reply.Success = true
	reply.FollowerTerm = s.node.currentTerm

	s.resetHeartbeatTicker()

	if reply.Success {
		for _, peer := range s.cluster.peers {
			if peer.id == payload.LeaderId {
				s.cluster.leader = peer
				return nil
			}
		}
	}
	return nil
}

func (s *Server) updateCommitIndex(leaderCommitIndex int, newEntries []LogEntry) {
	if leaderCommitIndex > s.node.commitIndex {
		lastLogIndex := newEntries[len(newEntries)-1].Index
		s.node.commitIndex = min(leaderCommitIndex, lastLogIndex)
	}
}

func (s *Server) updateEntries(newEntries []LogEntry) {
	s.removeIncoherentEntries(newEntries)
	// Append new entries
	for _, newEntry := range newEntries {
		s.node.Entries = append(s.node.Entries, &newEntry)
		s.logger.Debug("applying committed entry from leader")
		s.applyCommittedEntry(&newEntry)
	}
}

func (s *Server) removeIncoherentEntries(newEntries []LogEntry) {
	incoherentLogIndex := -1

	for _, newEntry := range newEntries {
		existingEntry := s.getLogEntryByIndex(newEntry.Index)
		if existingEntry != nil && existingEntry.Term != newEntry.Term {
			incoherentLogIndex = existingEntry.Index
		}
	}

	if incoherentLogIndex != -1 {
		arrayIndex := s.getLogArrayIndexByLogIndex(incoherentLogIndex)
		updatedLogs := make([]*LogEntry, len(s.node.Entries))
		if arrayIndex == 0 {
			arrayIndex++
		}
		copy(updatedLogs, s.node.Entries[:arrayIndex])
		s.node.Entries = updatedLogs
	}
}

func (s *Server) runFollower() {
	s.logger.Debug("running FOLLOWER", "node", s.node)
	s.restartHeatbeatTicker()
	for {
		<-s.listenHeartbeatTicker.C
		s.mutex.Lock()
		if s.node.state == FOLLOWER {
			s.mutex.Unlock()
			s.handleHeartbeatTimeout()
			return
		}
		s.mutex.Unlock()
		return
	}
}

func (s *Server) restartHeatbeatTicker() {
	if s.listenHeartbeatTicker != nil {
		s.listenHeartbeatTicker.Stop()
	}
	s.listenHeartbeatTicker = time.NewTicker(randomDuration(s.configuration.minHeartbeatTimeout, s.configuration.maxHeartbeatTimeout))
}

func (s *Server) resetHeartbeatTicker() {
	if s.listenHeartbeatTicker != nil {
		s.listenHeartbeatTicker.Reset(randomDuration(s.configuration.minHeartbeatTimeout, s.configuration.maxHeartbeatTimeout))
	}
}

func (s *Server) handleHeartbeatTimeout() {
	s.mutex.Lock()
	s.logger.Info("timedout waiting for heartbeat (lost leader)", "node", s.node)
	s.becomeCandidate()
	s.mutex.Unlock()
}

func (s *Server) runCandidate() {
	s.logger.Debug("running CANDIDATE", "node", s.node)
	if s.electionTicker != nil {
		s.electionTicker.Stop()
	}
	s.electionTicker = time.NewTicker(s.configuration.electionTimeout * _tickerUnit)
	s.mutex.Lock()
	if s.node.state == CANDIDATE {
		s.logger.Debug("requesting votes", "node", s.node)
		s.mutex.Unlock()
		for _, element := range s.cluster.peers {
			s.requestVoteWaitGroup.Add(1)
			go s.requestVote(element)
		}
	} else {
		s.mutex.Unlock()
		return
	}
	// Wait for all votes
	done := make(chan struct{})
	go func() {
		s.requestVoteWaitGroup.Wait()
		close(done)
	}()
	for {
		select {
		case <-s.electionTicker.C:
			s.logger.Info("timedout during election", "node", s.node)
			s.node.votes = 0
			s.node.votedFor = _nilPeerId
			return
		case <-done:
			s.mutex.Lock()
			if s.node.state != CANDIDATE {
				s.mutex.Unlock()
				return
			}
			s.processElectionResults()
			s.mutex.Unlock()
		}
	}
}

func (s *Server) processElectionResults() {
	livePeers := s.livePeers()
	majority := (livePeers+1)/2 + 1
	if s.node.votes >= majority {
		s.logger.Debug("won election", "node", s.node)
		s.becomeLeader()
		return
	} else {
		s.logger.Debug("lost election", "node", s.node)
		s.becomeFollower(s.node.currentTerm)
		return
	}
}

func (s *Server) livePeers() int {
	count := 0
	for _, peer := range s.cluster.peers {
		if peer.client != nil && peer.client.IsConnected() {
			count++
		}
	}
	return count
}

func (s *Server) requestVote(peer *Peer) {
	defer s.requestVoteWaitGroup.Done()
	if !peer.client.IsConnected() {
		peer.client.Reconnect()
	}
	if peer.client.IsConnected() {
		s.logger.Debug("requesting vote from peer", "node", s.node, "peer", peer.id)
		payload := RequestVotePayload{
			CandidateTerm:         s.node.currentTerm,
			CandidateId:           s.node.id,
			CandidateLastLogIndex: s.getLastLogIndex(),
			CandidateLastLogTerm:  s.getLastLogTerm(),
		}
		reply, _ := peer.client.RequestVote(payload)
		s.logger.Debug("requested vote from peer", "node", s.node, "reply", reply)
		if reply.FollowerTerm > s.node.currentTerm {
			s.logger.Debug("response term > current term", "node", s.node, "peer", peer.id)
			s.mutex.Lock()
			s.becomeFollower(reply.FollowerTerm)
			s.mutex.Unlock()
			return
		}
		if reply.FollowerVoteGranted {
			s.mutex.Lock()
			s.node.votes++
			s.mutex.Unlock()
		}
	}
}

func (s *Server) runLeader() {
	s.logger.Debug("running LEADER", "node", s.node)
	if s.sendHeartbeatTicker != nil {
		s.sendHeartbeatTicker.Stop()
	}
	heartbeatInterval := s.configuration.minHeartbeatTimeout / 2
	s.sendHeartbeatTicker = time.NewTicker(heartbeatInterval * _tickerUnit)
	for {
		<-s.sendHeartbeatTicker.C
		s.sendHeartbeats()
		s.mutex.Lock()
		if s.node.state != LEADER {
			s.mutex.Unlock()
			s.sendHeartbeatTicker.Stop()
			return
		}
		s.mutex.Unlock()
	}
}

func (s *Server) sendHeartbeats() {
	s.mutex.Lock()
	if s.node.state != LEADER {
		s.mutex.Unlock()
		return
	}
	s.mutex.Unlock()
	for _, peer := range s.cluster.peers {
		peer.client.Reconnect()
		if peer.client.IsConnected() {
			go s.sendEmptyEntries(peer)
		}
	}
}

func (s *Server) sendEmptyEntries(peer *Peer) {
	s.mutex.Lock()
	s.logger.Debug("sending heartbeat", "node", s.node, "peer", peer.id)
	prevLogEntryIndex := s.node.nextIndex[peer.id] - 1
	prevLogEntryTerm := -1
	prevLogEntry := s.getLogEntryByIndex(prevLogEntryIndex)
	if prevLogEntry != nil {
		prevLogEntryTerm = prevLogEntry.Term
	}
	payload := AppendEntriesPayload{
		LeaderTerm:        s.node.currentTerm,
		LeaderId:          s.node.id,
		PrevLogIndex:      prevLogEntryIndex,
		PrevLogTerm:       prevLogEntryTerm,
		Entries:           []LogEntry{},
		LeaderCommitIndex: s.node.commitIndex,
	}
	s.mutex.Unlock()
	reply, _ := peer.client.AppendEntries(payload)
	s.mutex.Lock()
	if reply.FollowerTerm > s.node.currentTerm {
		s.logger.Debug("response term > current term", "node", s.node, "peer", peer.id, "peerTerm", reply.FollowerTerm)
		s.becomeFollower(s.node.currentTerm)
	}
	s.mutex.Unlock()
	return
}

func (s *Server) becomeFollower(term int) {
	s.logger.Debug("transition to FOLLOWER", "node", s.node)
	s.node.state = FOLLOWER
	s.node.votes = 0
	s.node.currentTerm = term
	s.node.votedFor = _nilPeerId
	s.persistState()
}

func (s *Server) becomeLeader() {
	s.logger.Debug("transition to became LEADER", "node", s.node)
	s.node.state = LEADER
	s.node.votes = 0
	s.node.votedFor = _nilPeerId
	s.cluster.leader = nil
	s.initializeLeaderIndexes()
	s.persistState()
}

func (s *Server) initializeLeaderIndexes() {
	s.node.nextIndex = make(map[int]int, len(s.cluster.peers))
	s.node.matchIndex = make(map[int]int, len(s.cluster.peers))
	for _, peer := range s.cluster.peers {
		s.node.nextIndex[peer.id] = s.getLastLogIndex() + 1
		s.node.matchIndex[peer.id] = 0
	}
}

func (s *Server) becomeCandidate() {
	s.logger.Debug("transition to became CANDIDATE", "node", s.node)
	s.cluster.leader = nil
	s.node.state = CANDIDATE
	s.node.currentTerm++
	s.node.votedFor = s.node.id
	s.node.votes = 1
	s.persistState()
}

func (s *Server) SubmitCommand(command []byte) bool {
	s.mutex.Lock()

	s.logger.Debug("submitted command", "node", s.node, "command", command)

	if s.node.state != LEADER {
		s.mutex.Unlock()
		return false
	}

	s.node.Entries = append(s.node.Entries, &LogEntry{Index: s.getLastLogIndex() + 1, Term: s.node.currentTerm, Command: command})

	s.logger.Debug("sending entries")

	s.mutex.Unlock()
	s.sendAppendEntries()
	return true
}

func (s *Server) sendAppendEntries() {
	s.mutex.Lock()
	if s.node.state != LEADER {
		s.mutex.Unlock()
		return
	}
	s.mutex.Unlock()
	successCounter := atomic.Int32{}
	for _, peer := range s.cluster.peers {
		peer.client.Reconnect()
		if peer.client.IsConnected() {
			s.appendEntriesWaitGroup.Add(1)
			go s.sendEntries(peer, &successCounter)
		}
	}
	s.logger.Debug("sent all requests, now waiting")
	// Wait for all requests to be made
	done := make(chan struct{})
	go func() {
		s.appendEntriesWaitGroup.Wait()
		close(done)
	}()
	select {
	case <-done:
		s.mutex.Lock()
		defer s.mutex.Unlock()
		if s.node.state != LEADER {
			return
		}
		s.logger.Debug("processing results")
		if len(s.node.Entries) != 0 {
			livePeers := s.livePeers()
			majority := (livePeers+1)/2 + 1
			if int(successCounter.Load()) >= majority {
				s.logger.Debug("logs commited")
				s.node.commitIndex = s.node.commitIndex + len(s.node.Entries)
				return
			} else {
				s.logger.Debug("logs not commited")
				return
			}
		}
		return
	}
}

func (s *Server) sendEntries(peer *Peer, successCounter *atomic.Int32) {
	defer s.appendEntriesWaitGroup.Done()
	s.mutex.Lock()
	s.logger.Debug("sending entries to peer", "node", s.node, "peer", peer.id)
	prevLogEntryIndex := s.node.nextIndex[peer.id] - 1
	prevLogEntryTerm := -1
	prevLogEntry := s.getLogEntryByIndex(prevLogEntryIndex)
	if prevLogEntry != nil {
		prevLogEntryTerm = prevLogEntry.Term
	}
	entries := make([]LogEntry, 0)
	peerNextLogIndex := s.node.nextIndex[peer.id]
	if peerNextLogIndex >= 0 {
		for i := peerNextLogIndex; i < len(s.node.Entries); i++ {
			entries = append(entries, *s.node.Entries[i])
		}
	}
	payload := AppendEntriesPayload{
		LeaderTerm:        s.node.currentTerm,
		LeaderId:          s.node.id,
		PrevLogIndex:      prevLogEntryIndex,
		PrevLogTerm:       prevLogEntryTerm,
		Entries:           entries,
		LeaderCommitIndex: s.node.commitIndex,
	}
	s.mutex.Unlock()
	reply, _ := peer.client.AppendEntries(payload)
	s.mutex.Lock()
	if reply.FollowerTerm > s.node.currentTerm {
		s.logger.Debug("response term > current term", "node", s.node, "peer", peer.id, "peerTerm", reply.FollowerTerm)
		s.becomeFollower(s.node.currentTerm)
	}
	// follower contained entry matching prevLogIndex and prevLogTerm
	if reply.Success {
		for _, entry := range entries {
			s.logger.Debug("entries committed")
			s.node.nextIndex[peer.id]++
			s.node.matchIndex[peer.id]++
			s.node.commitIndex = entry.Index
			s.applyCommittedEntry(&entry)
			s.logger.Debug("after apply entry")
		}
		successCounter.Add(1)
	} else {
		s.node.nextIndex[peer.id]--
	}
	s.mutex.Unlock()
	return
}

func (s *Server) applyCommittedEntry(entry *LogEntry) {

	s.logger.Debug("applying entry to state machine", "entry", entry)

	commandResult, commitErr := s.stateMachine.Apply(entry.Command)
	if commitErr != nil {
		s.logger.Error("failed applying committed command to state machine", "err", commitErr, "command", string(entry.Command), "state machine", s.stateMachine)
	}
	s.node.lastApplied = entry.Index
	s.persistState()
	s.logger.Debug("before sending to committed entry channel")
	go func(committedEntry *LogEntry, committedEntriesChannel chan CommittedEntry) {
		committedEntriesChannel <- CommittedEntry{
			Index:   committedEntry.Index,
			Command: committedEntry.Command,
			Result:  commandResult,
			Error:   commitErr,
		}
	}(entry, s.CommittedCommandsChannel)

	s.logger.Debug("after sending to committed entry channel")
}

func (s *Server) persistState() {
	if s.storage != nil {
		s.logger.Debug("persisting state")
		state := &ServerNodeState{
			CurrentTerm: s.node.currentTerm,
			VotedFor:    s.node.votedFor,
			Entries:     s.node.Entries,
		}
		err := s.storage.Save(state)
		if err != nil {
			s.logger.Error("Failed to persist node state", "node", s.node, "error", err)
		}
	}
}

func (s *Server) getLogEntryByIndex(logIndex int) *LogEntry {

	arrayIndex := s.getLogArrayIndexByLogIndex(logIndex)

	if arrayIndex == -1 {
		return nil
	}

	return s.node.Entries[arrayIndex]
}

func (s *Server) getLastLog() *LogEntry {
	if len(s.node.Entries) == 0 {
		return nil
	}
	return s.node.Entries[len(s.node.Entries)-1]
}

func (s *Server) getLastLogIndex() int {
	if len(s.node.Entries) == 0 {
		return -1
	}
	return s.node.Entries[len(s.node.Entries)-1].Index
}

func (s *Server) getLastLogTerm() int {
	if len(s.node.Entries) == 0 {
		return -1
	}
	return s.node.Entries[len(s.node.Entries)-1].Term
}

func (s *Server) getLogArrayIndexByLogIndex(logIndex int) int {

	if len(s.node.Entries) == 0 {
		return -1
	}

	firstLogIndex := s.node.Entries[0].Index
	arrayIndex := logIndex - firstLogIndex

	if arrayIndex < 0 || arrayIndex >= len(s.node.Entries) {
		return -1
	}

	return arrayIndex
}

func (s *Server) getEntriesFromIndex(logIndex int) []*LogEntry {
	arrayIndex := s.getLogArrayIndexByLogIndex(logIndex)

	if arrayIndex != -1 {
		return s.node.Entries[arrayIndex:]
	}

	return nil
}

func randomDuration(min time.Duration, max time.Duration) time.Duration {
	if max.Milliseconds()-min.Milliseconds() <= 0 {
		return (max + min) * _tickerUnit
	}
	return time.Duration(rand.Int63n(max.Milliseconds()-min.Milliseconds())+min.Milliseconds()) * _tickerUnit
}
