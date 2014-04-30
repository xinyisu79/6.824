
/*
==============================
Note:

1)
 A danger: suppose in some view S1 is the primary; the viewservice changes views so that S2 is the primary;
 but S1 hasn't yet heard about the new view and thinks it is still primary. Then some clients might talk to S1,
 and others talk to S2, and not see each others' Put()s.

 2) Filter out duplicated request
 for real world implementation, server maybe using a sliding window to record processed request

 3) Network partition
 for primary server, always assign returned view to its current view, even when return empty view. for reason,
 see tick(); tick is tricky also due to a deadlock bug below

==============================

 Design:

 1) Filter duplicated request
 due to unsafety of network, request maybe unreached to the server, or reply doesn't reach client.
 add unique id for each request, server contain the outstanding(unresponded request) map[client] request
 assume each client has only one unresponded request

 PutArgs contains Me field, identifying client, which is passed by client during PutExt() arg inti();
 client.me is generated using nrand() in MakeClerk()/client.go


==============================
  QA:
 1) error return
 why return error has to be &Pberror{"msg"}, not Pberror{"msg"}

 2) is it right to ping view server when new client doesn't know what current view is?
 would view server take client as new avaliable server?

 3) filter out duplciated request
 then the backup server may also face same problem: its operation response hasn't reached primary?

 4) concurrent issue
 seems the concurrency does not fail the concurrent test... but should it be? finer grandularity lock?
 again, both partA B has problem of mutex protect data. better solution?

*/

package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"
import (
	"errors"
	"strconv"
)

//import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type PBServer struct {
	l net.Listener
	dead bool // for testing
	unreliable bool // for testing
	me string
	vs *viewservice.Clerk
	done sync.WaitGroup
	finish chan interface{}

	// Your declarations here.
	view viewservice.View

	// seen.client-name => uuid of outstanding rpc
	// oldreply.client-name => reply value of last rpc for outstanding client
	// key => value
	content map[string] string


	mu sync.Mutex // used to protect concurrent map access
}




func (pb *PBServer) isPrimary() bool{
	return pb.view.Primary == pb.me
}

func (pb *PBServer) isBackup() bool{
	return pb.view.Backup == pb.me
}

func (pb *PBServer) hasPrimary() bool{
	return pb.view.Primary != ""
}

func (pb *PBServer) hasBackup() bool{
	return pb.view.Backup != ""
}

func (pb *PBServer) Forward(args *ForwardArgs) error {
	if !pb.hasBackup(){
		return nil
	}
	var reply ForwardReply
	ok := call(pb.view.Backup, "PBServer.ProcessForward", args, &reply)
	if !ok {
		return errors.New("[Foward] failed to forward put")
	}
	return nil
}

func (pb *PBServer) ProcessForward(args *ForwardArgs, reply *ForwardReply) error{
	pb.mu.Lock()
	if !pb.isBackup(){
		pb.mu.Unlock()
		return errors.New("I'm not backup")
	}
	for key,value := range args.Content{
		pb.content[key] = value
	}
	pb.mu.Unlock()
	return nil
}


func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.

	//not primary case
	pb.mu.Lock()
	if !pb.isPrimary(){
		reply.Err = ErrWrongServer
		pb.mu.Unlock()
		return errors.New("i'm nothing")
	}

	key, value, client, uid := args.Key, args.Value, args.Me, args.UUID

	//at-most-once case
	if pb.content["seen." + client] == uid{
		reply.PreviousValue = pb.content["oldreply." + client]
		pb.mu.Unlock()
		return nil
	}

	if args.DoHash{
		reply.PreviousValue = pb.content[key]
		value = strconv.Itoa(int(hash(reply.PreviousValue + value)))
	}

	// forward added entries before commit
	forwardArgs := &ForwardArgs{map[string]string{
		key : value,
			"seen."+client : uid,
			"oldreply."+client : reply.PreviousValue}}
	err := pb.Forward(forwardArgs)
	if err != nil{
		pb.mu.Unlock()
		return errors.New("forward fail")
	}
	for key, value := range forwardArgs.Content{
		pb.content[key] = value
	}
	pb.mu.Unlock()
	return nil
}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	pb.mu.Lock()
	if !pb.isPrimary() {
		reply.Err = ErrWrongServer
		pb.mu.Unlock()
		return errors.New("[get] i'm not primary, received Get request")
	}
	reply.Value = pb.content[args.Key]
	pb.mu.Unlock()
	return nil
}


// ping the viewserver periodically.
func (pb *PBServer) tick() {
	// Your code here.
	pb.mu.Lock()
//	fmt.Println("[tick]: ", pb.me, " ", pb.view)
	view, err := pb.vs.Ping(pb.view.Viewnum)

	// Note: this is very tricky!
	// if the current primary server is parititioned in the network could not contact server
	// do nothing, still assign empty view_service to pb.view(see below)
	// becuase this would prevent this old primary server serve client's request (view ="", isPrimary() false)
	// it's ok to keep serve request with the [0, primaryDeadInterval) range after disconnected with view server
	// becuase for get: no harm, (backup not promoted yet); for put(), would succeed when is connected to backup

	// so: do nothing below
	if err != nil {
		//		fmt.Println("[PBServer.tick()]: ", pb.me, " ", view)
		//		os.Exit(-1)
	}

	//remove pb.isPrimary would cause deadlock... :(
	needForward := view.Backup != "" && view.Backup != pb.view.Backup && pb.isPrimary()
	//send the whole database to backup

	pb.view = view
	if needForward {
		pb.Forward(&ForwardArgs{Content:pb.content})
	}
	pb.mu.Unlock()
}


// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.finish = make(chan interface{})
	// Your pb.* initializations here.
	pb.content = map[string]string{}

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me);
	if e != nil {
		log.Fatal("listen error: ", e);
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63() % 1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63() % 1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", pb.me)
		pb.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}
