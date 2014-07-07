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
//"strconv"

/*
Design:

1. Each server has its corresponding paxos server


Notes:
1. Always assign Max() + 1 to next instance, so seq number begins from 1, 2, 3 ... so apply start from
	processed + 1.

2. Complex Issue: At most once
	one client issue a request to s1, s1 got block(mutex), and unreliable, disregard response.
	client then request to s2, s2 finish quickly and respond. Then client move on, make requests r2, r3 ...
	so if naively rememeber only one uuid, later on requests would overwrite that value, make s2 forget r1 is already
	processed...
	Current Solutions is: client request r1 -> s1 fail, sleep a while, and then issue next request. This ensures former
	request already processed. Then client leave Put() Get() Method, there is no outstanding request on server side for
	this request any more.
	So tricky... 

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

