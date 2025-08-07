package main

import (
	"RaftGo/raft"
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"sync"
)

var _ raft.StateMachine = (*KeyValueDbStateMachine)(nil)

type KeyValueDbStateMachine struct {
	mutex sync.Mutex
	store map[string]string
}

type KeyValueDbStateMachineCommandType = int

const (
	SET = iota
	GET
	UNSET
)

type KeyValueDbStateMachineCommand struct {
	commandType KeyValueDbStateMachineCommandType
	key         string
	value       string
}

type KeyValueDbStateMachineCommandResult struct {
	data any
}

func (s *KeyValueDbStateMachine) Apply(serializedCommand []byte) ([]byte, error) {

	command, err := deserializeCommand(serializedCommand)
	if err != nil {
		return nil, err
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()
	if command == nil {
		return nil, errors.New("command is nil")
	}

	if command.key == "" {
		return nil, fmt.Errorf("key \"%s\" is invalid", command.key)
	}

	switch command.commandType {
	case SET:
		s.store[command.key] = command.value
		return nil, nil
	case GET:
		return serializeCommandResult(&KeyValueDbStateMachineCommandResult{data: s.store[command.key]})
	case UNSET:
		value := s.store[command.key]
		delete(s.store, command.key)
		return serializeCommandResult(&KeyValueDbStateMachineCommandResult{data: value})
	default:
		return nil, fmt.Errorf("command \"%s\" is not recognized.", command.commandType)
	}
}

func deserializeCommand(command []byte) (*KeyValueDbStateMachineCommand, error) {
	var deserialized KeyValueDbStateMachineCommand
	decoder := gob.NewDecoder(bytes.NewBuffer(command))
	err := decoder.Decode(&deserialized)
	if err != nil {
		return nil, err
	}
	return &deserialized, nil
}

func serializeCommandResult(result *KeyValueDbStateMachineCommandResult) ([]byte, error) {
	var serialized bytes.Buffer
	encoder := gob.NewEncoder(&serialized)
	err := encoder.Encode(result)
	if err != nil {
		return nil, err
	}
	return serialized.Bytes(), nil
}

func (s *KeyValueDbStateMachine) Snapshot() ([]byte, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(s.store)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (s *KeyValueDbStateMachine) Restore(snapshot []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	var store map[string]string
	decode := gob.NewDecoder(bytes.NewBuffer(snapshot))
	err := decode.Decode(&store)
	if err != nil {
		return err
	}
	s.store = store
	return nil
}

func NewKeyValueDbStateMachine() *KeyValueDbStateMachine {
	return &KeyValueDbStateMachine{
		store: make(map[string]string),
	}
}
