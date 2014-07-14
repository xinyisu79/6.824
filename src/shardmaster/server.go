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
import "runtime/debug"

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

Note:

1. Rebanlancing

The most tricky part is rebalancing. General way would be calculate how many shards each replica group should have, (
 quota = num_of_shards / num_of_group, more = num_shards - quota * num_group) the top-more groups with most shards are
 assigned quota + 1, others are quota shards.) Then move from need less to group need more.

 However, when implementing this, found go is not convenient... some expected features not avaliable from build-in func.
  like a) get map.keys() b) sort according to certain field. has to write C-like troublesome code..

  So, instead, 'cause only possible scenario to cause unbalanced shards allocation is join/leave. just move from/to
   max/min with this joining/leaving group.



Problem & Question:

1. just allow config number keep going up?
seems so

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

func (sm *ShardMaster) CheckValid(c Config) {
	if len(c.Groups) > 0 {
		for _, g := range c.Shards {
			_, ok := c.Groups[g]
			if ok == false {
				fmt.Println("Not valid result, unallocated shards",c.Num)
				fmt.Println("len(groups): ", len(c.Groups))
				debug.PrintStack()
				os.Exit(-1)
			}
		}
	}
}

func GetGidCounts(c *Config) (int64, int64){
	min_id, min_num, max_id, max_num := int64(0),999,int64(0),-1
	counts := map[int64]int{}
	for g := range c.Groups{
		counts[g] = 0
	}
	for _, g := range c.Shards{
		counts[g]++
	}
	for g := range counts{
		_, exists := c.Groups[g]
		if exists && min_num > counts[g]{
			min_id, min_num = g, counts[g]
		}
		if exists && max_num < counts[g]{
			max_id, max_num = g, counts[g]
		}
	}
	for _, g := range c.Shards{
		if g == 0{
			max_id = 0
		}
	}
	return min_id, max_id
}

func GetShardByGid(gid int64, c *Config) int{
	for s, g := range c.Shards{
		if g == gid{
			return s
		}
	}
	return -1
}


//rebalancing the workload of shards among different groups
func (sm *ShardMaster) Rebalance(group int64, isLeave bool){
	c := &sm.configs[sm.cfgnum]
	for i := 0; ; i++{
		min_id, max_id := GetGidCounts(c)
		if isLeave{
			s := GetShardByGid(group, c)
			if s == -1{
				break
			}
			c.Shards[s] = min_id
		} else{
			if i == NShards / len(c.Groups){
				break
			}
			s := GetShardByGid(max_id, c)
			c.Shards[s] = group
		}
	}
}

// get new configuration based on old one
func (sm *ShardMaster) NextConfig() *Config{
	old := &sm.configs[sm.cfgnum]
	var new Config
	new.Num = old.Num + 1
	new.Groups = map[int64][]string{}
	new.Shards = [NShards]int64{}
	for gid, servers := range old.Groups {
		new.Groups[gid] = servers
	}
	for i, v := range old.Shards{
		new.Shards[i] = v
	}
	sm.cfgnum++
	sm.configs = append(sm.configs, new)
	return &sm.configs[sm.cfgnum]
}

func (sm *ShardMaster) ApplyJoin(gid int64, servers []string){
	config := sm.NextConfig()
	_, exists := config.Groups[gid]
	if !exists{
		config.Groups[gid] = servers
		sm.Rebalance(gid, false)
	}
}

func (sm *ShardMaster) ApplyLeave(gid int64){
	config := sm.NextConfig()
	_, exists := config.Groups[gid]
	if exists{
		delete(config.Groups, gid)
		sm.Rebalance(gid, true)
	}
}

func (sm *ShardMaster) ApplyMove(gid int64, shard int){
	config := sm.NextConfig()
	config.Shards[shard] = gid
}

func (sm *ShardMaster) ApplyQuery(num int) Config{
	if num == -1{
		sm.CheckValid(sm.configs[sm.cfgnum])
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
	sm.CheckValid(sm.configs[sm.cfgnum])
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{Type:LeaveOp, GID:args.GID}
	sm.AddOp(op)
	sm.CheckValid(sm.configs[sm.cfgnum])
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{Type:MoveOp, GID:args.GID, Shard:args.Shard}
	sm.AddOp(op)
	sm.CheckValid(sm.configs[sm.cfgnum])
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{Type:QueryOp, Num:args.Num}
	reply.Config = sm.AddOp(op)
	//	sm.CheckValid(sm.configs[sm.cfgnum])
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
