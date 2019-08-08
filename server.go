package DHT

import (
	"log"
	"math/big"
	"net"
	"net/rpc"
	"time"
)

func (n *Node) Get(k string) (bool, string) {
	var val string
	for i := 0; i < 5; i++ {
		_ = n.GetVal(&k, &val)
		if val != "" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	return val != "", val
}

func (n *Node) Put(k string, v string) bool {
	var tmp bool
	kvp := KVPair{k, v}
	_ = n.PutVal(&kvp, &tmp)
	return true
}

func (n *Node) Del(k string) bool {
	var tmp bool
	_ = n.DelVal(&k, &tmp)
	return true
}

func (n *Node) Run() {
	n.status = 1
	n.server = rpc.NewServer()
	_ = n.server.Register(n)

	var err error = nil
	n.listener, err = net.Listen("tcp", n.Info.IPAddr)
	if err != nil {
		log.Fatal("listen error: ", err)
	}

	go n.server.Accept(n.listener)
	go n.stabilize()
	go n.fixFingers()
	go n.checkPredecessor()
}

func (n *Node) Create() {
	if len(n.data) == 0 {
		n.data = make(map[string]KVPair)
	}
	for i := 0; i < M; i++ {
		n.Finger[i], n.Successors[i] = copyInfo(n.Info), copyInfo(n.Info)
	}
}

func (n *Node) Join(addr string) bool {
	client, err := n.Connect(InfoType{addr, big.NewInt(0)})
	if err != nil {
		return false
	}
	
	var other InfoType
	err = client.Call("Node.GetNodeInfo", 0, &other)
	if err != nil {
		return false
	}
	
	n.mutex.Lock()
	err = client.Call("Node.FindSuccessor", n.Info.NodeNum, &n.Successors[0])
	n.Finger[0] = copyInfo(n.Successors[0])
	n.mutex.Unlock()
	
	_ = client.Close()

	client, err = n.Connect(n.Successors[0])
	if err != nil {
		return false
	}
	
	var tmp int
	err = client.Call("Node.Notify", &n.Info, &tmp)
	if err != nil {
		return false
	}
	
	var pred InfoType
	err = client.Call("Node.GetPredecessor", 0, &pred)
	if err != nil {
	    return false
	}
	
	err = client.Call("Node.TransferData", &pred, &tmp)
	if err != nil {
		return false
	}
	
	_ = client.Close()
	return true
}

func (n *Node) Quit() {
	_ = n.listener.Close()
	n.status = 0

	err := n.FindFirstSuccessor(nil, &n.Successors[0])
	if err != nil {
		return
	}
	
	var tmp int
	err = n.TransferDataForce(&n.Successors[0], &tmp)
	
	if err != nil {
		return
	}
	
	client, err = n.Connect(n.Predecessor)
	if err != nil {
		return
	}
	
	err = client.Call("Node.ModifySuccessors", &n.Successors[0], &tmp)
	_ = client.Close()
	if err != nil {
		return
	}
	
	client, err = n.Connect(n.Successors[0])
	if err != nil {
		return
	}
	
	err = client.Call("Node.ModifyPredecessor", &n.Predecessor, &tmp)
	_ = client.Close()
	if err != nil {
		return
	}
	
	n.clear()
}

func (n *Node) ForceQuit() {
	_ = n.listener.Close()
	n.status = 0
}

func (n *Node) Ping(addr string) bool {
	if addr == n.Info.IPAddr {
		return n.status > 0
	}

	client, err := n.Connect(InfoType{addr, big.NewInt(0)})
	if err != nil {
		return false
	}

	var success int
	_ = client.Call("Node.GetStatus", 0, &success)
	_ = client.Close()
	return success > 0
}

func (n *Node) Dump() {

}

func (n *Node) GetStatus(_ *int, reply *int) error {
	*reply = n.status
	return nil
}

func (n *Node) Createaddr(addr string) {
	var t = GetHash(addr)
	n.Info = InfoType{addr, t}
	n.Predecessor = InfoType{"", big.NewInt(0)}
}

func (n *Node) AppendTo(k string, v string) {
	tmp, t := n.Get(k)
	if tmp {
		n.Put(k, t+v)
	}
}

func (n *Node) clear() {
	for i := 0; i < M; i++ {
		n.Successors[i] = InfoType{"", big.NewInt(0)}
	}
}
