package pbservice

import "hash/fnv"
import "crypto/rand"
import "math/big"

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongServer = "ErrWrongServer"
)
type Err string

type PutArgs struct {
  Key string
  Value string
  DoHash bool // For PutHash
  // You'll have to add definitions here.

  // Field names must start with capital letters,
  // otherwise RPC will break.

	Forward bool // identify whether it's primary => backup forward request
}

type PutReply struct {
  Err Err
  PreviousValue string // For PutHash
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
}

type GetReply struct {
  Err Err
  Value string
}

// Your RPC definitions here.

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
