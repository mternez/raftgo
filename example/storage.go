package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"raftgo/raft"
	"sync"
	"time"
)

var _ raft.ServerNodeStorage = (*JsonStorage)(nil)

const _filePerm = 0644

type PersistentState struct {
	Timestamp   time.Time            `json:"timestamp"`
	Checksum    string               `json:"checksum"`
	ServerState raft.ServerNodeState `json:"state"`
}

type JsonStorage struct {
	FileName string
	State    *PersistentState
	mu       sync.Mutex
}

func NewJsonStore(fileName string) *JsonStorage {

	file, err := os.OpenFile(fileName, os.O_RDWR, _filePerm)

	if err != nil {
		// Storage file does not exist
		return createInitialStorageFile(fileName)
	}

	defer file.Close()

	// Storage file does exist, read the current persisted state
	fileBytes, readErr := io.ReadAll(file)

	if readErr != nil {
		panic(readErr)
	}

	var existingState PersistentState

	err = json.Unmarshal(fileBytes, &existingState)

	if err != nil {
		panic(err)
	}

	// Compute and validate checksum
	checksum, checksumErr := computeStateChecksum(&(existingState.ServerState))

	if checksumErr != nil {
		panic(checksumErr)
	}

	if checksum != existingState.Checksum {
		panic(fmt.Errorf("checksum mismatch : stored %s but expected %s", existingState.Checksum, checksum))
	}

	return &JsonStorage{FileName: fileName, State: &existingState, mu: sync.Mutex{}}
}

func createInitialStorageFile(fileName string) *JsonStorage {

	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, _filePerm)

	if err != nil {
		panic(err)
	}

	defer file.Close()

	// Create initial node state
	initialServerState := raft.ServerNodeState{}

	// Compute checksum
	checksum, err := computeStateChecksum(&initialServerState)

	if err != nil {
		panic(err)
	}

	// Create initial state
	initialState := &PersistentState{
		Timestamp:   time.Now(),
		Checksum:    checksum,
		ServerState: initialServerState,
	}

	marshaledBytes, marshalError := json.Marshal(initialState)

	if marshalError != nil {
		panic(marshalError)
	}

	_, err = file.Write(marshaledBytes)

	if err != nil {
		panic(err)
	}

	return &JsonStorage{FileName: fileName, State: initialState, mu: sync.Mutex{}}
}

func computeStateChecksum(state *raft.ServerNodeState) (string, error) {

	// Encode struct
	var buffer bytes.Buffer

	encoder := gob.NewEncoder(&buffer)

	err := encoder.Encode(state)

	if err != nil {
		return "", err
	}

	hashCalculator := sha256.New()

	_, err = hashCalculator.Write(buffer.Bytes())

	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", hashCalculator.Sum(nil)), err
}

func (s *JsonStorage) Save(state *raft.ServerNodeState) error {
	return s.AtomicWrite(state)
}

func (s *JsonStorage) Fetch() (*raft.ServerNodeState, error) {
	return &(s.State.ServerState), nil
}

func (s *JsonStorage) AtomicWrite(state *raft.ServerNodeState) error {

	if err := validateState(state); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	tempFile := s.FileName + ".tmp"

	newChecksum, err := computeStateChecksum(state)

	if err != nil {
		return err
	}

	newPersistentState := &PersistentState{
		Timestamp:   time.Now(),
		Checksum:    newChecksum,
		ServerState: *state,
	}

	marshaledState, marshalErr := json.Marshal(newPersistentState)

	if marshalErr != nil {
		return marshalErr
	}

	err = os.WriteFile(tempFile, marshaledState, _filePerm)

	if err != nil {
		return err
	}

	err = syncFile(tempFile)

	if err != nil {
		os.Remove(tempFile)
		return err
	}

	s.State = newPersistentState

	if err := os.Rename(tempFile, s.FileName); err != nil {
		os.Remove(tempFile)
		return err
	}

	return nil
}

func validateState(state *raft.ServerNodeState) error {
	if state.CurrentTerm < 0 {
		return fmt.Errorf("invalid currentTerm: %d", state.CurrentTerm)
	}
	if state.VotedFor < -1 {
		return fmt.Errorf("invalid votedFor: %d", state.VotedFor)
	}
	for i, entry := range state.Entries {
		if entry == nil {
			return fmt.Errorf("nil entry at index %d", i)
		}
		if entry.Index < 0 || entry.Term < 0 {
			return fmt.Errorf("invalid entry at index %d: Index=%d, Term=%d",
				i, entry.Index, entry.Term)
		}
	}
	return nil
}

func syncFile(path string) error {

	file, err := os.OpenFile(path, os.O_RDWR, _filePerm)

	if err != nil {
		return err
	}

	defer file.Close()

	return file.Sync()
}
