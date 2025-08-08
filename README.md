# raftgo

Basic Raft implementation in Golang

## Project structure

The `raft` package contains the implementation of the Raft algorithm.

The `example` folder contains a `main` package which is a usage example of this implementation.

## Usage example

`example` folder contains an example use case of the raft algorithm.
The example in this case is an in memory key-value database (just a simple map[string]string). 

### Structure 

- cfg1.yml, cfg2.yml, cfg3.yml, cfg4.yml contain different node configurations used in this example package.
  - Using these configurations you can easily setup a cluster of up to 4 nodes to test this implementation
- kvstatemachine.go contains a simple in memory key value database serving as a statemachine.
- rpcclient.go contains the RPC client used in the example.
- storage.go contains a simple json file storage system
- configuration.go contains utilities to read yaml node configuration from the configuration files

### How to run

The following command starts the example program with the configuration for the node of id "1" :

````
cd example
go run . cfg1.yml
````

Upon running any node, a file ``store_[NODE_ID].json`` will be created to persist the state of the node.

### Exposed endpoints on example project

An HTTP server is ran alongside the raft server (port specified by the httpPort property in the configuration file).

4 endpoints are exposed for setting, getting and unsetting values in the sample Key Value Database state machine.

The following endpoints all expect a body of the following format :  { Key: string, Value: string }

- POST /set 
- GET  /get
- POST /unset

The following endpoint returns a string representation of the current values in the state machine.

- GET /all

## Done

- Leader election
- Log replication
- Persistence without log compaction

## To Do

Raft :
- Expose Metrics
- Tests

Main :
- Create a client that fetches the state of all available nodes and compares states