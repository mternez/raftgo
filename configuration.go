package main

import (
	"gopkg.in/yaml.v3"
	"log"
	"os"
)

type ServerCfg struct {
	MinHeartbeatTimeout int `yaml:"minHeartbeatTimeout"`
	MaxHeartbeatTimeout int `yaml:"maxHeartbeatTimeout"`
	ElectionTimeout     int `yaml:"electionTimeout"`
	ProbePeersTimeout   int `yaml:"probePeersTimeout"`
	Node                NodeCfg
	Peers               []NodeCfg `yaml:"peers"`
}

type NodeCfg struct {
	Id   int    `yaml:"id"`
	Host string `yaml:"host"`
	Port string `yaml:"port"`
}

func ReadConfiguration(fileName string) *ServerCfg {

	cfgFile, err := os.ReadFile(fileName)

	if err != nil {
		log.Fatal("Couldn't open ", fileName, " : ", err)
	}

	var cfg ServerCfg

	err = yaml.Unmarshal(cfgFile, &cfg)

	if err != nil {
		log.Fatal("Format error in ", fileName, " : ", err)
	}

	return &cfg
}
