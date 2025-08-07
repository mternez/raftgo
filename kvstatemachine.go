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
	SET KeyValueDbStateMachineCommandType = iota
	GET
	UNSET
)

type KeyValueDbStateMachineCommand struct {
	CommandType KeyValueDbStateMachineCommandType
	Key         string
	Value       string
}

type KeyValueDbStateMachineCommandResult struct {
	Data string
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

	if command.Key == "" {
		return nil, fmt.Errorf("Key \"%s\" is invalid", command.Key)
	}

	switch command.CommandType {
	case SET:
		s.store[command.Key] = command.Value
		return serializeCommandResult(&KeyValueDbStateMachineCommandResult{Data: s.store[command.Key]})
	case GET:
		return serializeCommandResult(&KeyValueDbStateMachineCommandResult{Data: s.store[command.Key]})
	case UNSET:
		value := s.store[command.Key]
		delete(s.store, command.Key)
		return serializeCommandResult(&KeyValueDbStateMachineCommandResult{Data: value})
	default:
		return nil, fmt.Errorf("command \"%s\" is not recognized.", command.CommandType)
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
