package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"

/*

Notes:
1. Settings:
	Each server has its corresponding paxos peer, content[] map cache result, processed identify watermark of applied
	operation.

	server get request r(op):
	1) From [processed+1, Max], must process all these requests before applying current one, because undecided instance
	in the range r could occur(unreliable, r processed not respond)
	2) hole instance h(unknown result for this server s1) in [processed + 1, Max]
		2.1) If h is decided in view of s1, just learn it and apply it
		2.2) If h is undecided in view of s1,
			a)it's decided by majority(but unknown to s1 due to loss when sending decision), put up op, paxos ensures it's
			still old decided request, not this op
			b)it's not decided by majority, just put up op at this hole does not matter: if client r0 <: r1, seq(r0) must
			  be before this hole. because this processing mechanism ensures no holes up to seq(r_current). =>
			  seq(r0) <: seq(hole) <:=  seq(r1) (if put up at hole, seq(hole) = seq(r1)


	However, the most complex issue is 2. At most once semantics...



2. Complex Issue: At most once
	client ck1 issue a request r1 to s1, s1 got block(mutex), waiting for other request and unreliable => disregard
	response. client then request to s2, s2 finish quickly and respond. Then client move on, make requests r2, r3 ...
	so if naively rememeber only one uuid, later on requests would overwrite that value, when s1's blocking on r1 get
	scheduled s1 forget r1 is already processed...

	Possible Solutions:
	A. Client wait (Currently adopted)
	client request r1 -> s1 fail, sleep a while, and then issue next request. This ensures former
	request already processed. Then client leave Put() Get() Method, there is no outstanding request on server side for
	this request any more.
	This depends on during client sleep interval, the blocked request on s1 get scheduled

	B. Server rememeber more
	Instead of just rememeber last one req, remember last-k requests. Then could tolerate overwrite k times, namely k
	more new ck1's request handled, before this last old request get scheduled.
	Depends on how many new request issued from client.
	Single put/get would be faster.
	Seems dymano adopt smiliar approach
	Variation may be remember last K miniute requests


	This problem would reappear in ManyPartition test case: partition means minority servers could not procceed, and
	client would more likely issue new request, making it forget that has been processed.

	waiting longer partly solve it...

	So tricky...

	However: in real world, partition could be very long, s1 receive r1, get partioned, system already process r2, r3, r4
	 .. r100.
	 if using solution A), then client wait time has to be longer than partitioned time.
	 if using solution B), then remember last-K should be larger than number of request issued by ck1 during partition

	 parition time has to be a upper bound... otherwise at-most-once is hard to acheive...

I'm so stupid: simple solution from https://github.com/colin2328/6.824/tree/master/src/kvpaxos :(
C) just making uuid from each clieng starting from 1, and keep increasing for following requests, 2, 3, 4...
	just compare the uuid value, if arg.UUID <= last seen[uuid], indicates it's already processed...



3. PL & Modeling
	It's hard to conceive corner case and debugging is troublesome. Automatic verification normally requires manual efforts
	for modeling. Better language design => modeling & coding & code gen & verification all embodied by language itself?



Problem:

1. TOKNOW: why the succeed, not break the loop of client? leads to multiple request success

**/


const (
	Debug=0
	GetOp = 1
	PutOp = 2
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type int
	Key string
	Value string
	Dohash bool
	UUID int64
	Client string
}

type KVPaxos struct {
	mu sync.Mutex
	l net.Listener
	me int
	dead bool // for testing
	unreliable bool // for testing
	px *paxos.Paxos

	// Your definitions here.
  content map[string] string //key-value content
	seen map[string] int64 // seen outstanding client request, maintaining at-most-once semantics
	replies map[string] string // map (client_name) => last outstanding reply result
	processed int // up to this seq number, all has been applied in content dict{}
}

func PrintOp(op Op) string{
	if op.Type == GetOp{
		return fmt.Sprintf("{c:%s get uid:%d key:%s}", op.Client, op.UUID%1000000, op.Key)
	}else{
		return fmt.Sprintf("{c:%s put uid:%d key:%s value:%s}", op.Client, op.UUID%1000000, op.Key, op.Value)
	}
}


func (kv *KVPaxos)WaitAgreement(seq int) Op{
	to := 10 * time.Millisecond
	for {
		decided, val := kv.px.Status(seq)
		if decided{
			return val.(Op)
		}
		time.Sleep(to)
		if to < 10 * time.Second{
			to *= 2
		}
	}
}

//Apply all operation entries before seq, not included
func (kv *KVPaxos) Apply(op Op, seq int) {
	previous, exists := kv.content[op.Key]
	if !exists{
		previous = ""
	}
	kv.replies[op.Client] = previous
	kv.seen[op.Client] = op.UUID

	if op.Type == PutOp{
		if op.Dohash{
			kv.content[op.Key] = NextValue(previous, op.Value)
//			fmt.Println("apply sr", kv.me, "ck", op.Client, "seq ", seq, "uuid", op.UUID%1000000, "key", op.Key,
//					"before", previous, "after", kv.content[op.Key])
		} else{
			kv.content[op.Key] = op.Value
		}
	}
	kv.processed++
	kv.px.Done(kv.processed)
}


func (kv *KVPaxos)AddOp(op Op) (bool, string) {
//	fmt.Printf("AddOp srv:%s recv %s\n", kv.me, PrintOp(op))
	var ok = false
	for !ok {
		//check seen
		uuid, exists := kv.seen[op.Client];
		if exists && uuid == op.UUID{
			return false, kv.replies[op.Client]
		}

		seq := kv.processed + 1
		decided, t := kv.px.Status(seq)
		var res Op
		if decided{
			res = t.(Op)
		} else{
			kv.px.Start(seq, op)
			res = kv.WaitAgreement(seq)
//			fmt.Println("sr ", kv.me, " put up ", seq, " seen ", exists, " seenUUID ", uuid, " opuuid", op.UUID)
		}
		ok = res.UUID == op.UUID
		kv.Apply(res, seq)
	}
	return true, kv.replies[op.Client]
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key, uuid, ck := args.Key, args.UUID, args.Me
	_, result := kv.AddOp(Op{Type:GetOp, Key:key, UUID:uuid, Client:ck})
	reply.Value = result
	return nil
}


func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key, value, dohash, uuid, ck := args.Key, args.Value, args.DoHash, args.UUID, args.Me
	_, result := kv.AddOp(Op{PutOp, key, value, dohash, uuid, ck})
	if dohash{
		reply.PreviousValue = result
	}
	return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.content = map[string]string{}
	kv.seen = map[string]int64{}
	kv.replies = map[string]string{}
	kv.processed = 0

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me]);
	if e != nil {
		log.Fatal("listen error: ", e);
	}
	kv.l = l


	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63() % 1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63() % 1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.dead == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}

