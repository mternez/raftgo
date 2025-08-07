package raft

type Logger interface {
	Debug(msg string, fields ...any)
	Info(msg string, fields ...any)
	Error(msg string, fields ...any)
}
