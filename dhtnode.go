package DHT

import (
	"errors"
	"math/big"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"time"
)

const M = 160

var expM = new(big.Int).Exp(big.NewInt(2), big.NewInt(M), nil)

type KVPair struct {
	Key   string
	Value string
}

type InfoType struct {
	IPAddr  string
	NodeNum *big.Int
}

type Node struct {
	Finger      [M]InfoType
	Successors  [M]InfoType
	Predecessor InfoType
	Info        InfoType
	data        map[string]KVPair
	server      *rpc.Server
	mutex       sync.Mutex
	status      int
	listener    net.Listener
	wg          sync.WaitGroup
}

func (n *Node) GetNodeInfo(_ *int, reply *InfoType) error {
	*reply = copyInfo(n.Info)
	return nil
}

func (n *Node) GetSuccessors(_ *int, reply *[M]InfoType) error {
	*reply = n.Successors
	return nil
}

func (n *Node) GetPredecessor(_ *int, reply *InfoType) error {
	if n.Predecessor.NodeNum.Cmp(big.NewInt(0)) != 0 && !n.Ping(n.Predecessor.IPAddr) {
		n.Predecessor = InfoType{"", big.NewInt(0)}
	}
	*reply = copyInfo(n.Predecessor)
	return nil
}

func (n *Node) ModifyPredecessor(pred *InfoType, reply *int) error {
	n.mutex.Lock()
	n.Predecessor = copyInfo(*pred)
	n.mutex.Unlock()
	return nil
}

func (n *Node) ModifySuccessors(succ *InfoType, _ *int) error {
//  fmt.Println(n.Successors[0])
	if succ.NodeNum.Cmp(n.Info.NodeNum) == 0 {
		return nil
	}
	
	client, err := n.Connect(*succ)
	if err != nil {
		return err
	}
	
	n.mutex.Lock()
	
	var SuccT [M]InfoType
	err = client.Call("Node.GetSuccessors", 0, &SuccT)
	if err != nil {
		return err
	}
	
	_ = client.Close()
	n.Finger[0], n.Successors[0] = copyInfo(*succ), copyInfo(*succ)
	for i := 1; i < M; i++ {
		n.Successors[i] = SuccT[i-1]
	}
//  fmt.Println(n.Successors[0])
	n.mutex.Unlock()
	return nil
}

func (n *Node) FindFirstSuccessor(tmp *int, reply *InfoType) error {
//  fmt.Println(n.Successors[0])
	n.mutex.Lock()
	for i, node := range n.Successors {
		if !n.Ping(node.IPAddr) {
			n.Successors[i] = InfoType{"", big.NewInt(0)}
			continue
		}
		*reply = copyInfo(node)
		n.mutex.Unlock()
		return nil
	}
	n.mutex.Unlock()
	return errors.New("No Successor")
}

func (n *Node) findPredecessor(id *big.Int) InfoType {
	var tmp_succ InfoType
	var tmp int
	err := n.FindFirstSuccessor(&tmp, &tmp_succ)
	if err != nil {
		return InfoType{"", big.NewInt(0)}
	}
//  fmt.Println(tmp_succ)
	cnt := 0
    pre := copyInfo(n.Info)
	for (!checkBetween(big.NewInt(1).Add(pre.NodeNum, big.NewInt(1)), tmp_succ.NodeNum, id)) && cnt <= 32 {
		cnt++
		if pre.NodeNum.Cmp(n.Info.NodeNum) != 0 {
			client, err := n.Connect(pre)
			if err != nil {
				return InfoType{"", big.NewInt(0)}
			}
			err = client.Call("Node.ClosestPredNode", id, &pre)
			if err != nil {
				_ = client.Close()
				return InfoType{"", big.NewInt(0)}
			}
			_ = client.Close()
		} else {
			err = n.ClosestPredNode(id, &pre)
		}
		
		if err != nil {
			return InfoType{"", big.NewInt(0)}
		}
		
		client, err := n.Connect(pre)
		if err != nil {
			return InfoType{"", big.NewInt(0)}
		}
		
		err = client.Call("Node.FindFirstSuccessor", 0, &tmp_succ)
		if err != nil {
			return InfoType{"", big.NewInt(0)}
		}
//      fmt.Println(pre)
		err = client.Close()
	}
	return pre
}

func (n *Node) ClosestPredNode(id *big.Int, reply *InfoType) error {
	for i := M-1; i >= 0; i-- {
		if checkBetween(big.NewInt(1).Add(n.Info.NodeNum, big.NewInt(1)), id, n.Finger[i].NodeNum) {
			if !n.Ping(n.Finger[i].IPAddr) {
				n.Finger[i] = InfoType{"", big.NewInt(0)}
				continue
			}
			*reply = copyInfo(n.Finger[i])
			return nil
		}
	}
// fmt.Println(id)
	for i := M-1; i >= 0; i-- {
		if checkBetween(big.NewInt(1).Add(n.Info.NodeNum, big.NewInt(1)), id, n.Successors[i].NodeNum) {
			if !n.Ping(n.Successors[i].IPAddr) {
				n.Successors[i] = InfoType{"", big.NewInt(0)}
				continue
			}
			*reply = copyInfo(n.Successors[i])
			return nil
		}
	}

	*reply = copyInfo(n.Info)
	return nil
}

func (n *Node) FindSuccessor(id *big.Int, reply *InfoType) error {
	pre := n.findPredecessor(id)
//  fmt.Println(pre)
	client, err := n.Connect(pre)
	if err != nil {
		return err
	}
	err = client.Call("Node.FindFirstSuccessor", 0, reply)
	_ = client.Close()
	return err
}

func (n *Node) DirectGet(k *string, reply *string) error {
	hash_tmp := GetHash(*k)
	n.mutex.Lock()
	val, tmp := n.data[hash_tmp.String()]
	n.mutex.Unlock()
	if !tmp {
		return errors.New("Can't get")
	}
	*reply = val.Value
	return nil
}

func (n *Node) GetVal(k *string, reply *string) error {
	hash_tmp := GetHash(*k)
	if val, ok := n.data[hash_tmp.String()]; ok {
		*reply = val.Value
		return nil
	}
	var succ InfoType
	err := n.FindSuccessor(hash_tmp, &succ)
	if err != nil {
		return err
	}
//  fmt.Println(succ)
	if succ.NodeNum.Cmp(n.Info.NodeNum) != 0 {
		client, err := n.Connect(succ)
		if err != nil {
			return err
		}
		var res string
		err = client.Call("Node.DirectGet", k, &res)
		if err != nil {
			_ = client.Close()
			return err
		}
		_ = client.Close()
		*reply = res
	}
	return nil
}

func (n *Node) DirectPut(kv *KVPair, reply *bool) error {
	hash_tmp := GetHash(kv.Key)
	n.mutex.Lock()
	n.data[hash_tmp.String()] = *kv
	n.mutex.Unlock()
	*reply = true
	return nil
}

func (n *Node) PutVal(kv *KVPair, reply *bool) error {
	hash_tmp := GetHash(kv.Key)
	var suc InfoType
	err := n.FindSuccessor(hash_tmp, &suc)

	if err != nil {
		return err
	}
//  fmt.Println(suc)
	var succ InfoType
	if suc.NodeNum.Cmp(n.Info.NodeNum) == 0 {
		n.data[hash_tmp.String()] = *kv
		*reply = true
		succ = n.Successors[0]
	} else {
		client, err := n.Connect(suc)
		if err != nil {
			return err
		}
		err = client.Call("Node.DirectPut", kv, reply)
		if err != nil {
			_ = client.Close()
			return err
		}
		_ = client.Call("Node.FindFirstSuccessor", 0, &succ)
		_ = client.Close()
	}
//  fmt.Println(succ)
	client, err := n.Connect(succ)
	if err != nil {
		return err
	}
	var tmp bool
	_ = client.Call("Node.DirectPut", kv, &tmp)
	_ = client.Close()
	return nil
}

func (n *Node) DirectDel(k *string, reply *bool) error {
	n.mutex.Lock()
	hash_tmp := GetHash(*k)
	_, ok := n.data[hash_tmp.String()]
	if ok {
		delete(n.data, hash_tmp.String())
	}
	*reply = ok
	n.mutex.Unlock()
	return nil
}

func (n *Node) DelVal(k *string, reply *bool) error {
	hash_tmp := GetHash(*k)
	var suc InfoType
	err := n.FindSuccessor(hash_tmp, &suc)
	if err != nil {
		return err
	}
//  fmt.Println(suc)
	if suc.NodeNum.Cmp(n.Info.NodeNum) == 0 {
		_, ok := n.data[hash_tmp.String()]
		if ok {
			delete(n.data, hash_tmp.String())
		}
		*reply = ok
	} else {
		client, err := n.Connect(suc)
		if err != nil {
			return err
		}
		err = client.Call("Node.DirectDel", k, reply)
		if err != nil {
			_ = client.Close()
			return err
		}
		_ = client.Close()
	}
	return nil
}

func (n *Node) TransferData(trans *InfoType, reply *int) error {
	if trans.IPAddr == "" {
		return nil
	}

	client, err := n.Connect(*trans)
	if err != nil {
		return err
	}

	n.mutex.Lock()
	for hashKey, KV := range n.data {
		var t big.Int
		t.SetString(hashKey, 10)
		if checkBetween(n.Info.NodeNum, trans.NodeNum, &t) {
			var tmp bool
			err := client.Call("Node.DirectPut", &KV, &tmp)
			if err != nil {
				n.mutex.Unlock()
				_ = client.Close()
				return err
			}
			delete(n.data, hashKey)
		}
	}
	n.mutex.Unlock()
	_ = client.Close()
	return nil
}

func (n *Node) TransferDataForce(trans *InfoType, reply *int) error {
	client, err := n.Connect(*trans)
	if err != nil {
		return err
	}

	n.mutex.Lock()
	for _, KV := range n.data {
		var tmp bool
		err := client.Call("Node.DirectPut", &KV, &tmp)
		if err != nil {
			n.mutex.Unlock()
			_ = client.Close()
			return err
		}
	}
	n.mutex.Unlock()
	_ = client.Close()
	return nil
}

func (n *Node) stabilize() {
	for {
		if n.status == 0 {
			break
		}
		var tmp int
		var x = InfoType{"", big.NewInt(0)}

		err := n.FindFirstSuccessor(nil, &n.Successors[0])
		if err != nil {
			continue
		}

		client, err := n.Connect(n.Successors[0])
		if err != nil {
			continue
		}
		err = client.Call("Node.GetPredecessor", 0, &x)
		if err != nil {
			continue
		}

		n.mutex.Lock()
		if x.NodeNum.Cmp(big.NewInt(0)) != 0 && checkBetween(big.NewInt(1).Add(n.Info.NodeNum, big.NewInt(1)), n.Successors[0].NodeNum, x.NodeNum) {
			n.Successors[0], n.Finger[0] = copyInfo(x), copyInfo(x)
		}
		n.mutex.Unlock()
		_ = client.Close()

		err = n.ModifySuccessors(&n.Successors[0], &tmp)
		if err != nil {
			continue
		}
		client, err = n.Connect(n.Successors[0])
		if err != nil {
			continue
		}

		err = client.Call("Node.Notify", &n.Info, &tmp)
		_ = client.Close()
		if err != nil {
			continue
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (n *Node) Notify(Dnode *InfoType, reply *int) error {
	n.mutex.Lock()
	if n.Predecessor.IPAddr == "" || checkBetween(big.NewInt(1).Add(n.Predecessor.NodeNum, big.NewInt(1)), n.Info.NodeNum, Dnode.NodeNum) {
		n.Predecessor = copyInfo(*Dnode)
		n.mutex.Unlock()
		if n.Predecessor.IPAddr != n.Info.IPAddr {
			client, err := n.Connect(n.Predecessor)
			if err != nil {
				return err
			}
			err = client.Call("Node.Maintain", 0, nil)
			_ = client.Close()
		}
	} else {
		n.mutex.Unlock()
	}
	return nil
}

func (n *Node) checkPredecessor() {
	for {
		n.mutex.Lock()
		if n.Predecessor.IPAddr != "" {
			if !n.Ping(n.Predecessor.IPAddr) {
				n.Predecessor = InfoType{"", big.NewInt(0)}
			}
		}
		n.mutex.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (n *Node) fixFingers() {
	for {
		if n.status == 0 {
			break
		}
		i := rand.Intn(M-1) + 1
		var tmp big.Int
		tmp.Add(n.Info.NodeNum, tmp.Exp(big.NewInt(2), big.NewInt(int64(i)), nil))
		if tmp.Cmp(expM) >= 0 {
			tmp.Sub(&tmp, expM)
		}

		err := n.FindSuccessor(&tmp, &n.Finger[i])
		if err != nil {
			continue
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (n *Node) Maintain(_ *int, _ *int) error {
	err := n.FindFirstSuccessor(nil, &n.Successors[0])
	if err != nil {
		return nil
	}

	n.mutex.Lock()
	tmpT := make(map[string]KVPair)
	for k, v := range n.data {
		tmpT[k] = v
	}
	n.mutex.Unlock()

	client, err := n.Connect(n.Successors[0])
	if err != nil {
		return nil
	}

	for hash, kv := range tmpT {
		tmp, _ := new(big.Int).SetString(hash, 10)
		if !checkBetween(n.Predecessor.NodeNum, n.Info.NodeNum, tmp) {
			continue
		}
		var reply bool
		_ = client.Call("Node.DirectPut", &kv, &reply)
	}
	_ = client.Close()
	return nil
}

func checkBetween(a, b, mid *big.Int) bool {
	if a.Cmp(b) >= 0 {
		return checkBetween(a, expM, mid) || checkBetween(big.NewInt(0), b, mid)
	}
	return mid.Cmp(a) >= 0 && mid.Cmp(b) < 0
}

func (this *Node) Connect(otherNode InfoType) (*rpc.Client, error) {
	if otherNode.IPAddr == "" {
		return nil, errors.New("invalid address")
	}

	c := make(chan *rpc.Client, 1)
	var err error
	var client *rpc.Client

	go func() {
		for i := 0; i < 3; i++ {
			client, err = rpc.Dial("tcp", otherNode.IPAddr)
			if err == nil {
				c <- client
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		c <- nil
	}()

	select {
	case client := <-c:
		if client != nil {
			return client, nil
		} else {
			return nil, errors.New("can't connect")
		}
	case <-time.After(333 * time.Millisecond):
		if err == nil {
			err = errors.New("can't connect")
		}
		return nil, err
	}
}
