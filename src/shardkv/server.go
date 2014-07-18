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

	terms: forward shard S from G1 to G2, client is ck, this called migration . tick() periodically check new config
	from shardmaster.

	a) actions before migrate
	have to wait seq_num <= paxos.Max() to be applied before sending, ensuring sending shard content[] is updated.
	(seq_num < Max() could have operation on S) it's like when applying Get/Put, instead of modifying content[] directly,
	 append paxos log, and then apply. (reason: due to partition, this server may not see paxos decision on previous
	 operations)

	b) when to migrate/reconfiguration
	one thing is important: reconfiguration is just operation proposal from shardmaster,
	whether it's come into effect is just a local replica group's decision. Even the config client see may be more
	recent than replica group has, doesn't matter.
	 But the replica group should reach consensus on relative order between put/get op and when to
	 reconfiguration/migration. As if sr1 use config2, return ck1's put(a) ErrGroup, sr2 use config1,
	 apply ck1's put(a) into effect. Than it's inconsistent.

	c) push or pull
	during reconfiguratoin cfg1 => cfg2, shard from G1 => G2, whose responsobility? G1 push or G2 pull when decide to
	apply reconfiguration? should be G2's pull: because right after reconfig op in G2,
	would have following put/get op required this migrated shard, if Push by G1, no gaurantee about when shard would
	arrive.


	d) accept all client[] status? (seen[], oldreplies[] map)
	no. if current G2.seen[ck] > migration.seen[ck], not update, otherwise update and modify seen[] correspondingly.
	This may cause modify seen[ck] that ck does not send to G2, (only request on Shard S should to G2 later) but does
	not matter to keep it as each client has only one outstanding request

	summary:
	shardmaster accept join/leave/move request, and make corresponding change, shardmaster is a group of servers using
	paxos to make it more fault tolerant. client and k/v servers learn the configration change by query(); however,
	it's not required for kv servers to finish shards migration before clients issue request based on new config. We
	could leave clients silly loop again and every replica turns down its request because none of them use as new
	config as client does... does not matter, client would keep looping and finally would succeed.

	Instead what's required is consens in a replica about when to do migration for reconfiguration,
	the reconfig's relative order to put/get should be same to all replica, it's a paxos problem. That's why put
	reconfig also as op in paxos log.

	e) prevent deadlock
	from test_test.go, it's possible, G1 in config7, G2 in config2, G2 cfg2 => cfg3 needs shard from G2,
	G1 from cfg7 => cfg8 needs shard from G1, so:
	G1, G2 both entering tick() holding lock, fail to serve other side's GetShard() request.(which also require lock)
	 Solution: when G1 request shard G2, in G2's GetShard() before acquire lock, check G2's current configuration
	 number, whether it's newer than G1's assumed. If not, just indicate not able, and G1's attempt to reconfigure
	 fail, return tick(), releasing lock().

	 This could produce a seriable reconfiguration order, which would not have deadlock problem. (remember: cfgn => cfg
	  n+1, if G1 <= (Shard S) G2, G2 would never require Shard from G1 (load-balancing mechanism of shardmaster
	  guarantee this).


	  Interesting but difficult project...
	  Distributed System is so tricky.... =_=

*/

const (
	Debug=0
	GetOp = 1
	PutOp = 2
	ReconfigOp = 3
	GetShardOp = 4
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
	SeqNum int64 // sequence number from certain client
	UUID int64 //unique identify the Op instance
	Client string
	Config shardmaster.Config //proposed new configration for ReconfigOp
	Reconfig GetShardReply //when ReconfigOp, what should be applied
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
	config shardmaster.Config
}



func PrintOp(op Op) string{
	switch op.Type{
	case PutOp:
		return fmt.Sprintf("put key:%s value:%s dohash:%t uuid:%d", op.Key, op.Value, op.Dohash, op.UUID)
	case GetOp:
		return fmt.Sprintf("get key:%s uuid:%d", op.Key, op.UUID)
	case GetShardOp:
		return fmt.Sprintf("gshard uuid:%d", op.UUID)
	case ReconfigOp:
		return fmt.Sprintf("recfg uuid:%d", op.UUID)
	}
	return ""
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


func (kv *ShardKV) ApplyGet(op Op){
	previous, _ := kv.content[op.Key]
	kv.replies[op.Client] = previous
	kv.seen[op.Client] = op.SeqNum
}

func (kv *ShardKV) ApplyPut(op Op){
	previous, _ := kv.content[op.Key]
	kv.replies[op.Client] = previous
	kv.seen[op.Client] = op.SeqNum

	if op.Dohash{
		kv.content[op.Key] = NextValue(previous, op.Value)
	} else{
		kv.content[op.Key] = op.Value
	}
}


func (kv *ShardKV) ApplyReconfigure(op Op){
	info := &op.Reconfig
	for key := range info.Content{
		kv.content[key] = info.Content[key]
	}
	for client := range info.Seen{
		seqnum, exists := kv.seen[client]
		if !exists || seqnum < info.Seen[client]{
			kv.seen[client] = info.Seen[client]
			kv.replies[client] = info.Replies[client]
		}
	}
	kv.config = op.Config
}

//Apply all operation entries before seq, not included
func (kv *ShardKV) Apply(op Op) {

	switch op.Type{
	case GetOp:
		kv.ApplyGet(op)
	case PutOp:
		kv.ApplyPut(op)
	case ReconfigOp:
		kv.ApplyReconfigure(op)
	case GetShardOp:
		//do nothing is fine
	}
	kv.processed++
	kv.px.Done(kv.processed)
}

func (kv *ShardKV) CheckOp(op Op) (Err, string){

	switch op.Type{
	case ReconfigOp:
		//check current config
		if kv.config.Num >= op.Config.Num{
			return OK, ""
		}
	case PutOp, GetOp:
		//check shard responsibility
		shard := key2shard(op.Key)
		if kv.gid != kv.config.Shards[shard]{
			return ErrWrongGroup, ""
		}

		//check seen
		seqnum, exists := kv.seen[op.Client]
		if exists && op.SeqNum <= seqnum{
			return OK, kv.replies[op.Client]
		}
	}
	return "", ""
}

func (kv *ShardKV)AddOp(op Op) (Err,string) {
	var ok = false
	op.UUID = nrand()
	for !ok {
		result, ret := kv.CheckOp(op)
		if result != ""{
			return result, ret
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
		kv.Apply(res)
	}
	return OK, kv.replies[op.Client]
}



func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key, seqnum, ck := args.Key, args.SeqNum, args.Me
	reply.Err, reply.Value = kv.AddOp(Op{Type:GetOp, Key:key, SeqNum:seqnum, Client:ck})
	return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{Type:PutOp, Key:args.Key, Value:args.Value,
		Dohash:args.DoHash, SeqNum:args.SeqNum, Client:args.Me}
	reply.Err, reply.PreviousValue = kv.AddOp(op)
	return nil
}

// G2 <= G1, shard S1, G2 call G1's GetShard()
func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) error{
	if kv.config.Num < args.Config.Num{
		reply.Err = ErrNotReady
		return nil
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard := args.Shard
	kv.AddOp(Op{Type:GetShardOp})

	reply.Content = map[string]string{}
	reply.Seen = map[string]int64{}
	reply.Replies = map[string]string{}

	for key := range kv.content{
		if key2shard(key) == shard{
			reply.Content[key] = kv.content[key]
		}
	}

	for client := range kv.seen {
		reply.Seen[client] = kv.seen[client]
		reply.Replies[client] = kv.replies[client]
	}

	return nil
}


func (reply *GetShardReply)Merge(other GetShardReply){
	for key := range other.Content{
		reply.Content[key] = other.Content[key]
	}
	for client := range other.Seen{
		uuid, exists := reply.Seen[client]
		if !exists || uuid < other.Seen[client]{
			reply.Seen[client] = other.Seen[client]
			reply.Replies[client] = other.Replies[client]
		}
	}
}

//return success or failure
func (kv *ShardKV) Reconfigure(newcfg shardmaster.Config) bool{
	//get shards

	reconfig := GetShardReply{OK, map[string]string{}, map[string]int64{},
		map[string]string{}}

	oldcfg := &kv.config
	for i := 0; i < shardmaster.NShards; i++{
		gid := oldcfg.Shards[i]
		if newcfg.Shards[i] == kv.gid && gid != kv.gid{
			args := &GetShardArgs{i, *oldcfg}
			var reply GetShardReply
			for _, srv := range oldcfg.Groups[gid]{
//				uid := nrand() % 10000
//				fmt.Printf("[%d] G%d(srv%d) <=(S%d) G%d(srv%d) cfg%d\n", uid, kv.gid, kv.me, i, gid, ind, newcfg.Num)
				ok := call(srv, "ShardKV.GetShard", args, &reply)
				if ok && reply.Err == OK{
//					fmt.Printf("[%d] done\n",uid)
					break
				}
				if ok && reply.Err == ErrNotReady{
					return false
				}
			}
			//process the replys, merge into one results
			reconfig.Merge(reply)
		}
	}
	op := Op{Type:ReconfigOp, Config:newcfg, Reconfig:reconfig}
	kv.AddOp(op)
	return true
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	newcfg := kv.sm.Query(-1)
	for i := kv.config.Num + 1; i <= newcfg.Num; i++{
		cfg := kv.sm.Query(i)
		if !kv.Reconfigure(cfg) {
			return
		}
	}
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
	kv.config = shardmaster.Config{Num:-1}

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
