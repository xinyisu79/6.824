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

//==================================================================================
Even though the following analysis is wrong, still want to keep it in some git commit
//==================================================================================


6. Migrating Shard

	terms: forward shard S from G1 to G2, client is ck, this called migration . tick() periodically check new config
	from shardmaster.

	a) actions before migrate
	have to wait seq_num <= paxos.Max() to be applied before sending, ensuring sending shard content[] is updated.
	(seq_num < Max() could have operation on S) it's like when applying Get/Put, instead of modifying content[] directly,
	 append paxos log, and then apply. (reason: due to partition, this server may not see paxos decision on previous
	 operations)

	b) when to migrate
	at first sight, it seems naturally to migrate during tick() config checking, however, it's not true.
	G1 is running on other shards processing, and not get into tick() to migrate S to G2, before migration happends,
	client request G2 for Put/Get. G2 fail to find entry in content[]. Wrong
	luckily, client send request before do shardmaster.Query(-1) (see client.go) , so it would always have chance to
	send request to Wrong group(that's responsible for S in old config). So when G1 saw ErrGroup,
	have opportunities and should migrate S to G2. (corner case: in first time, ck.config[S] = 0,
	ok; after getting first config, ck.config[S] = G1, but G1 not know about it, (not call tick() update config yet),
	it's also ok to return ErrWrongGroup, as ck would still send request to G1, and the content[] is empty,
	so does not matter.

	c) at-most-once migration?
	under above discussion, it's possible several severs in G1 all trying to migrate, but for replicas in G2,
	should accept migration from G1 only once.
	Senarios of multiple migrate: ck -> sr1 in G1, ErrGroup(not update UUID), unreliable, no response,
	sr1 migrate S => G2; ck -> sr2@G1, try to forward again.
	map[shard] => from_what_config_num_that_this_group_have_the_shard to record,
	pigpack this number along with migration request

	Should be at-most-once:
	ck1(a) cause sr1 G1 => G2, unreliable; ck2(b) => G2 update (ck1/ck2 different keys, same shard);
	ck1(a) => sr2 G2, migrate => G2. then update in ck2(b) would be lost.

	d) accept all client[] status? (seen[], oldreplies[] map)
	no. if current G2.seen[ck] > migration.seen[ck], not update, otherwise update and modify seen[] correspondingly.
	This may cause modify seen[ck] that ck does not send to G2, (only request on Shard S should to G2 later) but does
	not matter to keep it as each client has only one outstanding request



7. shared paoxs?
	according to code in test_test.go, shardmaster and k/v replica servers use seperate sets of paxos peers,
	so reconfigure and put/get operation are sperately ordered, not in same log stream. So it's possible that cfg1 =>
	cfg2, but servers/clients still use cfg1 to record. for correctness it's ok if consistency & orderings between
	config and operations.

Hint: Think about when it is ok for a server to give shards to the other server during view change.


Plan:
Firstly try simplest migration: sender/receiver both does not track shard they handled, always migrate when see
ErrGroup



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
	config shardmaster.Config
}

type MigratedShard struct {
	Content map[string] string
	Seen map[string] int64
	Replies map[string] string
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




func (kv *ShardKV) AcceptMigration(migration MigratedShard) error{
	return nil
}

/**
just need to do once waiting and apply operation whose seq_num < paxos.Max(); then sent the shard S to new group. No
need to worry about this group G would have accept new op in before sending result, as once we have reach the new config
where this group not responsible shard S, other replica in G would not accept operation for shard S anymore.
*/
func (kv *ShardKV) Migrate(shard int){
	//waits for opeartions to finish before migration
	//TODO: how to deal with unknown paxos instance? put up NO-OP?


	migrated :=  MigratedShard{Content:map[string]string{},
		Seen:map[string]int64{}, Replies:map[string]string{}}
	for k := range kv.content{
		if key2shard(k) == shard{
			migrated.Content[k] = kv.content[k]
		}
	}
	for c := range kv.seen{
		migrated.Seen[c] = kv.seen[c]
		migrated.Replies[c] = kv.replies[c]
	}
}


func (kv *ShardKV) CheckOwnership(key string) Err{
	shard := key2shard(key)
	//should try to migrate shards here
	if kv.config.Shards[shard] != kv.gid{
		kv.Migrate(shard)
		return ErrWrongGroup
	}
	return OK
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key, uuid, ck := args.Key, args.UUID, args.Me
	reply.Err = kv.CheckOwnership(key)
	if reply.Err == OK {
		result := kv.AddOp(Op{Type:GetOp, Key:key, UUID:uuid, Client:ck})
		reply.Value = result
	}
	return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key, value, dohash, uuid, ck := args.Key, args.Value, args.DoHash, args.UUID, args.Me
	reply.Err = kv.CheckOwnership(key)
	if reply.Err == OK {
		result := kv.AddOp(Op{PutOp, key, value, dohash, uuid, ck})
		if dohash{
			reply.PreviousValue = result
		}
	}
	return nil
}


//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	newcfg := kv.sm.Query(-1)
	if newcfg.Num > kv.config.Num{
		kv.config = newcfg
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
