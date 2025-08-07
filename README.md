# raftgo

Basic Raft implementation in Golang

## Project structure

The `raft` package contains the implementation of the Raft algorithm.

The `main` package contains an example for using this implementation.

## Example package

`main` package contains an example use case of the raft algorithm. 

### Structure 

- cfg1.yml, cfg2.yml, cfg3.yml, cfg4.yml contain different node configurations used in this example package.
  - Using these configurations you can easily setup a cluster of up to 4 nodes to test this implementation
- kvstatemachine.go contains a simple in memory key value database serving as a statemachine.
- rpcclient.go contains the RPC client used in the example.
- storage.go contains a simple json file storage system
- configuraiton.go contains utilities to read yaml node configuration from the configuration files

### How to run

The following command starts the example program with the configuration for the node of id "1" :

````
go run cfg1.yml
````

Upon running any node, a file ``store_[NODE_ID].json`` will be created to persist the state of the node.

## Done

- Leader election
- Log replication
- Persistence without log compaction

## To Do

Raft : 
- Apply commands to the state machine
- Redirect to the leader if node is requested by client or if old leader is requested
- Expose Metrics
- Tests 

Example `mainpackage` :
- Expose endpoints to send commands to the nodes