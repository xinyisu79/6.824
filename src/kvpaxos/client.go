package kvpaxos

import "net/rpc"
//import "fmt"
import "strconv"
import "time"

type Clerk struct {
  servers []string
  // You will have to modify this struct.
	//client's idenfication
	me string
}


func MakeClerk(servers []string) *Clerk {
  ck := new(Clerk)
  ck.servers = servers
  // You'll have to add code here.
	ck.me = strconv.FormatInt(nrand(), 10)
	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()
    
  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }

//  fmt.Println(err)
  return false
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
  // You will have to modify this function.
	arg := GetArgs{key, nrand(), ck.me}
	var reply GetReply
	succeed := false
	servInex := 0
	for !succeed{
		succeed = call(ck.servers[servInex], "KVPaxos.Get", arg, &reply)
		if succeed{
//			fmt.Printf("ck:%s send/get key %s srv:%d %d succ\n", ck.me, arg.Key, servInex, arg.UUID % 10000)
			return reply.Value
		} else{
//			fmt.Printf("ck:%s send/get key %s srv:%d %d fail\n", ck.me, arg.Key, servInex, arg.UUID % 10000)
		}
		servInex = (servInex + 1) % len(ck.servers)
		time.Sleep(time.Second * 2)
	}
	return reply.Value
}

//
// set the value for a key.
// keeps trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
  // You will have to modify this function.
	arg := PutArgs{key, value, dohash, nrand(), ck.me}
	var reply PutReply
	succeed := false
	servInex := 0
	for !succeed{
		succeed = call(ck.servers[servInex], "KVPaxos.Put", arg, &reply)
		if succeed{
//			fmt.Printf("ck:%s send/put key %s srv:%d %d succ\n", ck.me, arg.Key, servInex, arg.UUID % 1000000)
			return reply.PreviousValue
		} else{
//			fmt.Printf("ck:%s send/put key %s srv:%d %d fail\n", ck.me, arg.Key, servInex, arg.UUID % 1000000)
		}
		servInex = (servInex + 1) % len(ck.servers)
		time.Sleep(time.Second * 2)
	}
	return reply.PreviousValue
}

func (ck *Clerk) Put(key string, value string) {
  ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
  v := ck.PutExt(key, value, true)
  return v
}

