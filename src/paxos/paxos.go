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


/*note:

1. It's possible for a process to receive Accept request before receiving Prepare request:
 because proposer would proceed to send accept after just get majority OK response for prepare
 request.

2. In sendAccept()/sendPrepare(): put arg, reply definition inside loop. otherwise, last iteration'
   success would shadow next iteration's failure.

3. take a glance at https://github.com/colin2328/6.824/tree/master/src/paxos :(

 Question:

 1. TestRPCCount(): why after adding mu.lock(), would solve the livelock (one proposer's prepare
   supercede another's)? still unclear, to me, only leader could solve this. Like this:
   https://github.com/jefesaurus/6824final/blob/master/src/paxos/paxos.go
   (Also another repos, can't find it now...)

 2. Why using direct function call could resolve the problem to pass the unreliable network?




*/

import "net"
import "net/rpc"
import "os"
import "syscall"
import "sync"
import "fmt"
import "time"
import "strconv"
import "math/rand"
import "log"


const (
	OK           = "OK"
	REJECT       = "REJECT"
	EMPTY_NUMBER = "0"
)

func Assert(condition bool){
	if !condition{
		os.Exit(-1)
	}
}

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
	dones []int //peers' done map
}


type Proposal struct {
	PNum string
	PValue interface {}
}


type PaxosArg struct {
	Pid int    //id for which instance of paxos
	PNum string
	PValue interface{}
  Done int //done value from the sender side, used when send decision
  Me int // identification of sender, index from the peers[] array
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
//	px.mu.Lock()
	if _, exists := px.instances[pid]; !exists{
		px.MakePaxosInstance(pid,nil)
	}
	px.instances[pid].accepted = proposal
	px.instances[pid].decided = true
//	px.mu.Unlock()
}

func (px *Paxos) MakePaxosInstance(id int, v interface{}) {
	px.instances[id] = &PaxosInstance{
		clientV:v,
		pid:id,decided:false,
		prepared:EMPTY_NUMBER,
		accepted:Proposal{PNum:EMPTY_NUMBER}}
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
		targets[t] = px.peers[t]
		acceptors = append(acceptors, px.peers[t])
	}
	return acceptors
}


//generate the increasing paxos proposal number
func (px *Paxos) GeneratePaxosNumber() string {
	//zsy's birthday, how stupid i am...
	begin := time.Date(2014, time.May, 6, 23, 0, 0, 0, time.UTC)
	duration := time.Now().Sub(begin)
	return strconv.FormatInt(duration.Nanoseconds(), 10) + "-" + strconv.Itoa(px.me)
}


func (px *Paxos) SendDecisionToAll(pid int, result Proposal) {
	arg := PaxosArg{Pid:pid, PNum:result.PNum, PValue:result.PValue, Done:px.dones[px.me], Me:px.me}
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
func (px *Paxos) sendPrepare(pid int, v interface {}) (bool, [] string, Proposal){
	acceptors := px.SelectMajority()
	pnum := px.GeneratePaxosNumber()

	retPNum := EMPTY_NUMBER
	retPValue := v
	num := 0

	for i, acceptor := range acceptors {
		arg := PaxosArg{Pid: pid, PNum: pnum, Me:px.me}
		var reply PaxosReply =PaxosReply{Result:REJECT}
		if i == px.me{
			px.ProcessPrepare(&arg, &reply)
		}else{
			call(acceptor, "Paxos.ProcessPrepare", &arg, &reply)
		}
		if reply.Result == OK {
			if reply.PNum > retPNum{
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
	num := 0
	for i, peer := range acceptors {
		arg := PaxosArg{Pid:pid, PNum:proposal.PNum, PValue:proposal.PValue, Me:px.me}
		var reply PaxosReply = PaxosReply{Result:REJECT}
		if i == px.me{
			px.ProcessAccept(&arg, &reply)
		} else{
			call(peer, "Paxos.ProcessAccept", &arg, &reply)
		}
		if reply.Result == OK {
			num++
		}
	}
	return px.IsMajority(num)
}

func (px *Paxos) doProposer(pid int, v interface {}) {
	var i int = 1
	for {
		ok, acceptors, proposal := px.sendPrepare(pid,v)
		if ok{
			ok = px.sendAccept(pid, acceptors, proposal)
		}
		if ok {
			px.SendDecisionToAll(pid, proposal)
			break
		}
		i++
	}
}


func (px *Paxos) ProcessPrepare(arg *PaxosArg, reply *PaxosReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	pid := arg.Pid
	pnum := arg.PNum
	reply.Result = REJECT
	_, exist := px.instances[pid]
	if !exist {
		px.MakePaxosInstance(pid, nil)
		reply.Result = OK
	} else {
		if px.instances[pid].prepared < pnum {
			reply.Result = OK
		}
	}
	if reply.Result == OK {
		reply.PNum = px.instances[pid].accepted.PNum
		reply.PValue = px.instances[pid].accepted.PValue
		px.instances[pid].prepared = pnum
	}
	return nil
}

func (px *Paxos) ProcessAccept(arg *PaxosArg, reply *PaxosReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	pid := arg.Pid
	pnum := arg.PNum
	pvalue := arg.PValue
	reply.Result = REJECT
	_, exits := px.instances[pid]
	if !exits{
		px.MakePaxosInstance(pid, nil)
	}
	if pnum >= px.instances[pid].prepared {
		px.instances[pid].accepted = Proposal{pnum, pvalue}
		px.instances[pid].prepared = pnum
		reply.Result = OK
	}
	return nil
}

func (px *Paxos) ProcessDecision(arg *PaxosArg, reply *PaxosReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	px.MakeDecision(arg.Pid, Proposal{arg.PNum,arg.PValue})
	px.dones[arg.Me] = arg.Done
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
//			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

//	fmt.Println(err)
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
		if seq < px.Min(){
			return
		}
//		px.MakePaxosInstance(seq, v)
		px.doProposer(seq, v)
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
//  Assert(px.instances[seq].decided)
	me := px.me
  if px.dones[me] < seq {
		px.dones[me] = seq
	}
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

	px.mu.Lock()
	defer px.mu.Unlock()
	min_point := px.dones[px.me]
	for k := range px.dones{
		if min_point > px.dones[k]{
			min_point = px.dones[k]
		}
	}

	for k, _ := range px.instances{
		if k <= min_point && px.instances[k].decided{
			delete(px.instances, k)
		}
	}

	return min_point + 1
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

	min := px.Min()
	if seq < min {
		return false, nil
	}
	px.mu.Lock()
	defer px.mu.Unlock()
	if _, exist := px.instances[seq]; !exist{
		return false, nil
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
	px.dones = make([]int, len(peers))
	for i := range peers{
		px.dones[i] = -1
	}

	log.SetFlags(log.Lmicroseconds)

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
//					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}


	return px
}
