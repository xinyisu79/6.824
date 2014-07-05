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
Design:

1. Each server has its corresponding paxos server


Notes:
1. Always assign Max() + 1 to next instance, so seq number begins from 1, 2, 3 ... so apply start from
	processed + 1.


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
	processed int // up to this seq number, all has been applied in content dict{}
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


func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key, uuid, ck := args.Key, args.UUID, args.Me
	item := Op{Type:GetOp, Key:key, UUID:uuid, Client:ck}
	var ok = false
	var seq = 0
	for !ok {
		seq = kv.px.Max() + 1
		kv.px.Start(seq, item)
		ok = kv.WaitAgreement(seq).UUID == uuid
	}

	//TODO: PutExt(), treat differently
	//Apply all operation entries before this operation
	for i := kv.processed + 1; i < seq; i++{
		entry := kv.WaitAgreement(i)
		if entry.Type == PutOp{
			kv.content[entry.Key] = entry.Value
		}
	}
	kv.processed = seq
	kv.px.Done(seq)

	value, exits := kv.content[key]
	if exits{
		reply.Value = value
	}
	return nil
}


//TODO: seems that PutExt() also needs to apply all previous operations
func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key, value, dohash, uuid, ck := args.Key, args.Value, args.DoHash, args.UUID, args.Me
	item := Op{PutOp, key, value, dohash, uuid, ck}
	var ok = false
	for !ok {
		seq := kv.px.Max() + 1
		kv.px.Start(seq, item)
		ok = kv.WaitAgreement(seq).UUID == uuid
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

