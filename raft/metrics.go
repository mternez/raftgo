package raft

type ServerMetrics struct {
	ElectionsStarted   int64
	ElectionsWon       int64
	HeartbeatsSent     int64
	HeartbeatsReceived int64
}

// TODO : Interface pour récolter les métriques
// TODO : Gauge, Counter, Histo
