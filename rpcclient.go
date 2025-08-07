package main

import (
	"RaftGo/raft"
	"fmt"
	"net/rpc"
)

var _ raft.PeerClient = (*RpcClient)(nil)

type RpcClient struct {
	host   string
	port   string
	client *rpc.Client
}

func (c *RpcClient) RequestVote(payload raft.RequestVotePayload) (*raft.RequestVoteReply, error) {
	var err error
	if c.client == nil {
		err = c.Reconnect()
	}
	if c.client != nil {
		var reply raft.RequestVoteReply
		callError := c.client.Call("Server.RequestVote", payload, &reply)
		if callError != nil {
			err = fmt.Errorf("call to Server.RequestVote failed (%s:%s): %s; %s", c.host, c.port, callError, err)
		}
		return &reply, err
	}
	return nil, err
}
func (c *RpcClient) AppendEntries(payload raft.AppendEntriesPayload) (*raft.AppendEntriesReply, error) {
	var err error
	if c.client == nil {
		err = c.Reconnect()
	}
	if c.client != nil {
		var reply raft.AppendEntriesReply
		callError := c.client.Call("Server.AppendEntries", payload, &reply)
		if callError != nil {
			err = fmt.Errorf("call to Server.AppendEntries failed (%s:%s): %s; %s", c.host, c.port, callError, err)
		}
		return &reply, err
	}
	return nil, err
}

func (c *RpcClient) Reconnect() error {
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%s", c.host, c.port))
	if err != nil {
		return fmt.Errorf("reconnection failed (%s:%s) : %s", c.host, c.port, err)
	}
	c.client = client
	return nil
}

func (c *RpcClient) IsConnected() bool {
	return c.client != nil
}

func NewRpcClient(host string, port string) (*RpcClient, error) {
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%s", host, port))
	if err != nil {
		return &RpcClient{host: host, port: port, client: nil}, err
	}
	return &RpcClient{host: host, port: port, client: client}, nil
}
