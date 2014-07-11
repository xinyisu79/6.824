package shardmaster

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

/**

Design:

1. Join/Leave/Move/Query, the ordering of reconfiguration relative to put/get should be considered
	Solution: they are also considered as operations as put/get, paxos to agree on operations and then.
	but they don't have to same type, as paxos support interface{} value as paxos instance agreed on items.

	The implication of above is: we use same paxos group for key/value shard server(store key/value map) and shardmaster.

	In test_test go for shardmaster, see multiple shardmaster(in lab4's description, only one shardmaster, maybe now just
	for test).

	And each shardmaster has corresponding paxos peer colated on same site(same kvh)
	(This does not matter, paxos is library as Lab 3, but using different goroutines to do accept/paxos operations,
	sperated with shardmaster's request/opeartion handling)


Problem & Question:

1. just allow config number keep going up?

*/


const (
	JoinOp = "Join"
	LeaveOp = "Leave"
	MoveOp = "Move"
	QueryOp = "Query"
)


type ShardMaster struct {
	mu sync.Mutex
	l net.Listener
	me int
	dead bool // for testing
	unreliable bool // for testing
	px *paxos.Paxos

	configs []Config // indexed by config num
	processed int // processed sequence number in paxos
	cfgnum int //current config number, largest
}


//all fileds' names have to start with capital letters by RPC requirements
type Op struct {
	// Your data here.
	Type string
	GID int64
	Servers []string
	Shard int
	Num int
	UUID int64
}



func (kv *ShardMaster)WaitAgreement(seq int) Op{
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

//rebalancing the workload of shards among different groups
func (sm *ShardMaster) Rebalance(){
	config := &sm.configs[sm.cfgnum]
	num := len(config.Groups)
	quota := NShards / num
	counts := map[int64]int{}
	gids := make([]int64, 0)

	for gid := range config.Groups{
		counts[gid] = 0
		gids = append(gids, gid)
	}
	for _, gid := range config.Shards{
		counts[gid]++
	}

	//brute force... since NShards is just 10
	for {
		ok := true
		for _, gid := range gids{
			if counts[gid] < quota{
				ok = false
				for shard, from := range config.Shards{
					_, exists := config.Groups[from]
					if counts[from] > quota || !exists{
						config.Shards[shard] = gid
						counts[gid]++
						if exists{
							counts[from]--
						}
						break
					}
				}
				break
			}
		}
		if ok{
			return
		}
	}
}

// get new configuration based on old one
func (sm *ShardMaster) NextConfig() *Config{
	old := &sm.configs[sm.cfgnum]
	var new Config
	new.Num = old.Num + 1
	new.Groups = map[int64][]string{}
	for gid, servers := range old.Groups {
		new.Groups[gid] = servers
	}
	new.Shards = old.Shards
	sm.cfgnum++
	sm.configs = append(sm.configs, new)
	return &sm.configs[sm.cfgnum]
}

func (sm *ShardMaster) ApplyJoin(gid int64, servers []string){
	config := sm.NextConfig()
	config.Groups[gid] = servers
	sm.Rebalance()
}

func (sm *ShardMaster) ApplyLeave(gid int64){
	config := sm.NextConfig()
	delete(config.Groups, gid)
	sm.Rebalance()
}

func (sm *ShardMaster) ApplyMove(gid int64, shard int){
	config := sm.NextConfig()
	config.Shards[shard] = gid
}

func (sm *ShardMaster) ApplyQuery(num int) Config{
//	fmt.Printf("query num: %d len %d", num, len(sm.configs))
	if num == -1{
		return sm.configs[sm.cfgnum]
	} else{
		return sm.configs[num]
	}
}



//Apply all operation entries before seq, not included
//when new configuration from old one, has to deep clone map, which is reference in golang.

func (sm *ShardMaster) Apply(op Op, seq int) Config{
	sm.processed++
	gid, servers, shard, num := op.GID, op.Servers, op.Shard, op.Num
	switch op.Type{
	case JoinOp:
		sm.ApplyJoin(gid, servers)
	case LeaveOp:
		sm.ApplyLeave(gid)
	case MoveOp:
		sm.ApplyMove(gid, shard)
	case QueryOp:
		return sm.ApplyQuery(num)
	default:
		fmt.Println("Unexpected opeation type for ShardMaster!")
	}
	sm.px.Done(sm.processed)
	return Config{}
}


func (sm *ShardMaster)AddOp(op Op) Config {
	op.UUID = nrand()
	for {
		seq := sm.processed + 1
		decided, t := sm.px.Status(seq)
		var res Op
		if decided{
			res = t.(Op)
		} else{
			sm.px.Start(seq, op)
			res = sm.WaitAgreement(seq)
		}
		config := sm.Apply(res, seq)
		if res.UUID == op.UUID{
			return config
		}
	}
}


//===================================
//Operation function: TODO should fill the fields for reply
//===================================


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{Type:JoinOp, GID:args.GID, Servers:args.Servers}
	sm.AddOp(op)
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{Type:LeaveOp, GID:args.GID}
	sm.AddOp(op)
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{Type:MoveOp, GID:args.GID, Shard:args.Shard}
	sm.AddOp(op)
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{Type:QueryOp, Num:args.Num}
	reply.Config = sm.AddOp(op)
	return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
	sm.dead = true
	sm.l.Close()
	sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	//your initialization
	sm.cfgnum = 0

	rpcs := rpc.NewServer()
	rpcs.Register(sm)

	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me]);
	if e != nil {
		log.Fatal("listen error: ", e);
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.dead == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.dead == false {
				if sm.unreliable && (rand.Int63() % 1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.unreliable && (rand.Int63() % 1000) < 200 {
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
			if err != nil && sm.dead == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
