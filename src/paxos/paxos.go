package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "time"
import "strconv"
import "math/rand"


const (
	OK           = "OK"
	REJECT       = "REJECT"
	EMPTY_NUMBER = "0"
)

type Paxos struct {
	mu sync.Mutex
	l net.Listener
	dead bool
	unreliable bool
	rpcCount int
	peers []string
	me int // index into peers[]

	// Your data here.
	instances map[int]*PaxosInstance //active paxos instances
}


type Proposal struct {
	PNum string
	PValue interface {}
}


type PaxosArg struct {
	Pid int    //id for which instance of paxos
	PNum string
	PValue interface{}
}

type PaxosReply struct {
	Result string
	PNum string
	PValue interface{}
}


type PaxosInstance struct {
	pid int
	clientV interface{}
	decided  bool
	prepared string
	accepted Proposal
}

func (px *Paxos) MakeDecision(pid int, proposal Proposal) {
	if _, exists := px.instances[pid]; !exists{
		px.MakePaxosInstance(pid,nil)
	}
	px.instances[pid].accepted = proposal
	px.instances[pid].decided = true
}

func (px *Paxos) MakePaxosInstance(id int, v interface{}) *PaxosInstance {
	inst := &PaxosInstance{}
	inst.clientV = v
	inst.pid = id
	inst.decided = false
	inst.prepared = EMPTY_NUMBER
	inst.accepted = Proposal{PNum:EMPTY_NUMBER}
	if px.instances == nil{
		fmt.Println("nil instance...")
	}
	px.instances[id] = inst
	return inst
}

func (px *Paxos) IsMajority(num int) bool {
	return num > len(px.peers)/2
}

func (px *Paxos) SelectMajority() []string {
	len := len(px.peers)
	size := int(len/2) + 1
	size = size + rand.Intn(len-size)
	targets := map[int]string{}
	acceptors := make([]string, 0)
	for i := 0; i < size; i++ {
		t := 0
		for {
			t = rand.Int() % len
			if _, exists := targets[t]; exists {
				continue
			}
			break
		}
		targets[i] = px.peers[t]
		acceptors = append(acceptors, px.peers[t])
	}
	return acceptors
}


//generate the increasing paxos proposal number
func (px *Paxos) GeneratePaxosNumber() string {
	//zsy's birthday, how stupid i am...
	begin := time.Date(1990, time.May, 6, 23, 0, 0, 0, time.UTC)
	duration := time.Now().Sub(begin)
	return strconv.FormatInt(duration.Nanoseconds(), 10) + " " + px.peers[px.me]
}


func (px *Paxos) SendDecisionToAll(pid int, result Proposal) {
	arg := PaxosArg{Pid:pid, PNum:result.PNum, PValue:result.PValue}
	reply := PaxosReply{}
	px.MakeDecision(pid, result)
	for i := range px.peers {
		if i != px.me{
			call(px.peers[i], "Paxos.ProcessDecision", &arg, &reply)
		}
	}
}


// sending prepare request
// @pid: paxos instance id
// return:
// @bool: succeess or not
// @[] string: acceptors that next should send to
// @Proposal: the proposal should send
func (px *Paxos) sendPrepare(pid int) (bool, [] string, Proposal){
	acceptors := px.SelectMajority()
	pnum := px.GeneratePaxosNumber()

	arg := PaxosArg{Pid: pid, PNum: pnum}
	var reply PaxosReply

	retPNum := pnum
	retPValue := px.instances[pid].clientV
	num := 0

	for _, acceptor := range acceptors {
		ok := call(acceptor, "Paxos.ProcessPrepare", &arg, &reply)
		if ok && reply.Result == OK {
			if reply.PNum != EMPTY_NUMBER || reply.PNum > retPNum{
				retPNum = reply.PNum
				retPValue = reply.PValue
			}
			num++
		}
	}
	return px.IsMajority(num), acceptors, Proposal{PNum:pnum, PValue:retPValue}
}


// sending accepted proposal to acceptors

func (px *Paxos) sendAccept(pid int, acceptors []string, proposal Proposal) bool {
	arg := PaxosArg{Pid:pid, PNum:proposal.PNum, PValue:proposal.PValue}
	var reply PaxosReply
	num := 0
	for i := range acceptors {
		ok := call(acceptors[i], "Paxos.ProcessAccept", &arg, &reply)
		if ok {
			num++
		}
	}
	return px.IsMajority(num)
}

func (px *Paxos) doProposer(pid int) {
	var instance = px.instances[pid]
	for !instance.decided {
		ok, acceptors, proposal := px.sendPrepare(pid)
		if ok{
			ok = px.sendAccept(pid, acceptors, proposal)
		}
		if ok {
//			px.MakeDecision(pid,proposal)
//			instance.decided = true
			px.SendDecisionToAll(pid, proposal)
		}
	}
}


func (px *Paxos) ProcessPrepare(arg *PaxosArg, reply *PaxosReply) error {
//	fmt.Println("prepare accepted")
	pid := arg.Pid
	pnum := arg.PNum
	instance, exist := px.instances[pid]
	reply.Result = REJECT
	if !exist {
		instance = px.MakePaxosInstance(pid, nil)
		reply.Result = OK
	} else {
		if instance.prepared < pnum {
			reply.Result = OK
		}
	}
	if reply.Result == OK {
		reply.PNum = instance.accepted.PNum
		reply.PValue = instance.accepted.PValue
		instance.prepared = pnum
	}
	return nil
}

func (px *Paxos) ProcessAccept(arg *PaxosArg, reply *PaxosReply) error {
	pid := arg.Pid
	pnum := arg.PNum
	pvalue := arg.PValue
	reply.Result = REJECT
	if pnum > px.instances[pid].prepared {
		px.instances[pid].accepted = Proposal{pnum, pvalue}
		reply.Result = OK
	}
	return nil
}

func (px *Paxos) ProcessDecision(arg *PaxosArg, reply *PaxosReply) error {
	px.MakeDecision(arg.Pid, Proposal{arg.PNum,arg.PValue})
	return nil
}



//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//func (px *Paxos) Process(arg *PaxosArg, reply *PaxosReply) error{
//	fmt.Println("receive process", arg.P.Pnum, " ", arg.P.Pvalue)
//	return nil
//}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	go func() {
		px.MakePaxosInstance(seq, v)
		px.doProposer(seq)
	}()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	largest := 0
	for k, _ := range px.instances {
		if k > largest{
			largest = k
		}
	}
	return largest
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
// 
func (px *Paxos) Min() int {
	// You code here.
	return 0
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
	// Your code here.
	if _, exist := px.instances[seq]; !exist{
		px.MakePaxosInstance(seq, nil)
	}
	if px.instances[seq].accepted.PValue == nil && px.instances[seq].decided{
		fmt.Println("fuck")
	}
	return px.instances[seq].decided, px.instances[seq].accepted.PValue
}


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me


	// Your initialization code here.
	px.instances = map[int]*PaxosInstance{}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me]);
		if e != nil {
			log.Fatal("listen error: ", e);
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63() % 1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63() % 1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}


	return px
}
