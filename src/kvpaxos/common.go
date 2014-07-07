package kvpaxos

import "hash/fnv"
import "crypto/rand"
import "math/big"
import "strconv"

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
)
type Err string

type PutArgs struct {
  // You'll have to add definitions here.
  Key string
  Value string
  DoHash bool  // For PutHash
  // You'll have to add definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  UUID int64
	Me string // identify clerk, for at-most-once semantics
}

type PutReply struct {
  Err Err
  PreviousValue string   // For PutHash
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
	UUID int64
	Me string
}

type GetReply struct {
  Err Err
  Value string
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

func nrand() int64 {
	max := big.NewInt(int64(int64(1) << 62))
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}


func NextValue(hprev string, val string) string {
	h := hash(hprev + val)
	return strconv.Itoa(int(h))
}
