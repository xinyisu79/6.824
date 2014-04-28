
/*
Note:

 A danger: suppose in some view S1 is the primary; the viewservice changes views so that S2 is the primary;
 but S1 hasn't yet heard about the new view and thinks it is still primary. Then some clients might talk to S1,
 and others talk to S2, and not see each others' Put()s.

 QA:
 1) error return
 why return error has to be &Pberror{"msg"}, not Pberror{"msg"}

 2) is it right to ping view server when new client doesn't know what current view is?
 would view server take client as new avaliable server?

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
import "errors"

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
	content map[string] string
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

func (pb *PBServer) Forward(args *PutArgs, reply *PutReply) error {
	if !pb.hasBackup(){
		return nil
	}
	args.Forward = true
	ok := call(pb.view.Backup, "PBServer.Put", args, &reply)
	if !ok {
		return errors.New("[Foward] failed to forward put")
	}
	return nil
}

func (pb *PBServer) ForwardAll() bool {
	for key,value := range pb.content {
		args := &PutArgs{Key:key, Value:value, DoHash:false, Forward:true}
		reply := &PutReply{}
		pb.Forward(args, reply)
	}
	return true
}

//TODO: handle duplicated request, adding fields in PutArgs field

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.

	var forwardReply PutReply
	key, value := args.Key, args.Value

	if pb.isPrimary(){
		if args.Forward{
			reply.Err = ErrWrongServer
			return errors.New("[put] primary receive Forward put")
		}
		err := pb.Forward(args, &forwardReply)
		if err != nil {
			return err
		}
		pb.content[key] = value
		return nil
	}
	if pb.isBackup(){
		if !args.Forward{
			reply.Err = ErrWrongServer
			return errors.New("[put] backup receive non-Forward put")
		}
		pb.content[key] = value
		return nil
	}
	reply.Err = ErrWrongServer
	return errors.New("i'm nothing")
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	if !pb.isPrimary() {
		reply.Err = ErrWrongServer
		return errors.New("[get] i'm not primary, received Get request")
	}
	reply.Value = pb.content[args.Key]
	return nil
}


// ping the viewserver periodically.
func (pb *PBServer) tick() {
	// Your code here.
	view, err := pb.vs.Ping(pb.view.Viewnum)
	if err != nil {
		fmt.Println("[PBServer.tick()]: ping view server fail")
		os.Exit(-1)
	}
	needForward := view.Backup != "" && view.Backup != pb.view.Backup
	//send the whole database to backup
	pb.view = view
	if needForward {
		pb.ForwardAll()
	}
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
