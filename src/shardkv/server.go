package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"


/*

Notes & Design (some from project description)

1. your server will need to periodically check with the shardmaster to see if there's a new configuration;
   do this in tick().

2. you should have a function whose job it is to examine recent entries in the Paxos log and apply them to the state
 of the shardkv server. Don't directly update the stored key/value database in the Put/Get handlers; instead, attempt
 to append a Put, PutHash, or Get operation to the Paxos log, and then call your log-reading function to find out what
 happened (e.g., perhaps a reconfiguration was entered in the log just before the Put/PutHash/Get).

3. respond ErrWrongGroup to Client if it's not responsible for corresponding shard.

4. at-most-once rpc
	a) during re-configuration, send map[client]seen, map[client]replies, to target group as well. but not always
	update, only arriving uuid is more recent that it has
	b) deal with ErrWrongGroup,
	 i) for client, should not change uuid, because: send to group G1, unreliable,
	 processed but no response; => migrating to group G2, so sending G1 get ErrGroup; but if send G2 with new uuid,
	 would have operation applied twice: once in G1 already, once in G2
	 ii) for server, should not change uuid either: client ck1 send G1, which does not have shard S1, if update with
	 uuid, seen[ck1] = uuid, and migrating some shard to server G2 that has s1, G2.seen[ck1] = uuid. Because client
	 would not change uuid as we see in i), so when seeing ck1's request for s1, G2 would assume it's already processed.
	  which is wrong.


5. after moving to new view, just leave shards not owing in new view there, undeleted, to simply implementation.

6. Migrating Shard
	Tick() periodically check whether the shard belongs itself, send shards to other group. But before sending content
	and user seen[] map, should apply all operations before(include) paxos.Max(). This is similar in Get()/Put()
	handler, has to try to append operation, instead of modify content[]map directly. Because due to unreliable
	network, it's possible for one peers fail to get paxos instance decision.
	On receiver side, could have map[gid] => processed_forward_config_num, to only process migration once,
	but seems multiple times does not matter. :)


Hint: Think about when it is ok for a server to give shards to the other server during view change.


*/

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
	Type int
	Key string
	Value string
	Dohash bool
	UUID int64
	Client string
}


type ShardKV struct {
	mu sync.Mutex
	l net.Listener
	me int
	dead bool // for testing
	unreliable bool // for testing
	sm *shardmaster.Clerk
	px *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	content map[string] string //key-value content
	seen map[string] int64 // seen outstanding client request, maintaining at-most-once semantics
	replies map[string] string // map (client_name) => last outstanding reply result
	processed int // up to this seq number, all has been applied in content dict{}
}


func (kv *ShardKV)WaitAgreement(seq int) Op{
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
func (kv *ShardKV) Apply(op Op, seq int) {
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

func (kv *ShardKV)AddOp(op Op) string {
	var ok = false
	for !ok {
		//check seen
		uuid, exists := kv.seen[op.Client];
		if exists && uuid == op.UUID{
			return kv.replies[op.Client]
		}

		seq := kv.processed + 1
		decided, t := kv.px.Status(seq)
		var res Op
		if decided{
			res = t.(Op)
		} else{
			kv.px.Start(seq, op)
			res = kv.WaitAgreement(seq)
		}
		ok = res.UUID == op.UUID
		kv.Apply(res, seq)
	}
	return kv.replies[op.Client]
}




func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	reply.Err = OK
	key, uuid, ck := args.Key, args.UUID, args.Me
	result := kv.AddOp(Op{Type:GetOp, Key:key, UUID:uuid, Client:ck})
	reply.Value = result
	return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	reply.Err = OK
	key, value, dohash, uuid, ck := args.Key, args.Value, args.DoHash, args.UUID, args.Me
	result := kv.AddOp(Op{PutOp, key, value, dohash, uuid, ck})
	if dohash{
		reply.PreviousValue = result
	}
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
}


// tell the server to shut itself down.
func (kv *ShardKV) kill() {
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
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
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.dead == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
