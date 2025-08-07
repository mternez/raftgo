package raft

const _nilPeerId = -1

type NodeState int

const (
	FOLLOWER NodeState = iota
	CANDIDATE
	LEADER
)

func (s NodeState) String() string {
	switch s {
	case FOLLOWER:
		return "FOLLOWER"
	case CANDIDATE:
		return "CANDIDATE"
	case LEADER:
		return "LEADER"
	default:
		return "UNKNOWN"
	}
}

type Cluster struct {
	peers  []*Peer
	leader *Peer
}

type Peer struct {
	id     int
	client PeerClient
}

type LogEntry struct {
	Term    int
	Index   int
	Command []byte
}

type PeerClient interface {
	RequestVote(payload RequestVotePayload) (*RequestVoteReply, error)
	AppendEntries(payload AppendEntriesPayload) (*AppendEntriesReply, error)
	Reconnect() error
	IsConnected() bool
	GetAddr() string
}

type RequestVotePayload struct {
	CandidateTerm         int
	CandidateId           int
	CandidateLastLogIndex int // index of candidate’s last log entry
	CandidateLastLogTerm  int // term of candidate’s last log entry
}

type RequestVoteReply struct {
	FollowerTerm        int
	FollowerVoteGranted bool
}

type AppendEntriesPayload struct {
	LeaderTerm        int        // leader’s term
	LeaderId          int        // so follower can redirect clients
	PrevLogIndex      int        // index of log entry immediately preceding new ones
	PrevLogTerm       int        // term of prevLogIndex entry
	Entries           []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommitIndex int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	FollowerTerm int  // currentTerm, for leader to update itself
	Success      bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func NewPeer(id int, client PeerClient) *Peer {
	return &Peer{
		id:     id,
		client: client,
	}
}
