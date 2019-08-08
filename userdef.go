package main

import (
	"test/chord"
	"strconv"
)

func NewNode(port int) dhtNode {
	var Nnode DHT.Node
	Nnode.Createaddr(DHT.GetLocalAddress() + ":" + strconv.Itoa(port))
	Nnode.Create()
	return &Nnode
}
