
package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
	mu sync.Mutex
	l net.Listener
	dead bool
	me string


	// Your declarations here.
	view View                           // current view on ViewServer
	primaryAck uint             // View Number acked by primary server
	ackTick uint
	backupTick uint
	currentTick uint
}



//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.

	num := args.Viewnum
	name := args.Me

	vs.mu.Lock()

	if vs.view.Primary == "" && vs.view.Viewnum == 0{
		vs.view.Primary = name
		vs.view.Viewnum = num + 1
		vs.ackTick = vs.currentTick
	} else if name == vs.view.Primary && num == 0 {
		vs.view.Primary = vs.view.Backup
		vs.view.Backup = ""
		vs.view.Viewnum++
	} else if name == vs.view.Primary && num != 0 {
		vs.primaryAck = num
		vs.ackTick = vs.currentTick
	} else if vs.view.Backup == "" && vs.view.Viewnum == vs.primaryAck{
		vs.view.Backup = name
		vs.view.Viewnum++
		vs.backupTick = vs.currentTick
	} else if name == vs.view.Backup && num == 0 && vs.view.Viewnum == vs.primaryAck{ //backup crash, still make it makeup
		vs.view.Backup = name
		vs.view.Viewnum++
		vs.backupTick = vs.currentTick
	} else if name == vs.view.Backup && num != 0 {
		vs.backupTick = vs.currentTick
	}
	reply.View = vs.view
	vs.mu.Unlock()

	return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	reply.View = vs.view
	vs.mu.Unlock()
	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	vs.mu.Lock()
	vs.currentTick++
	if vs.currentTick - vs.ackTick >= DeadPings{
		if vs.view.Backup == ""{
			vs.view.Primary = ""
			fmt.Println("no backup server when primary lost")
		} else if vs.primaryAck == vs.view.Viewnum {
			vs.view.Primary = vs.view.Backup
			vs.view.Backup = ""
			vs.view.Viewnum++
			vs.primaryAck = vs.backupTick
			vs.ackTick = vs.backupTick
		}
	}
	if vs.view.Backup != "" && vs.currentTick - vs.backupTick >= DeadPings && vs.view.Viewnum == vs.primaryAck{
		vs.view.Backup = ""
		vs.view.Viewnum++
	}
	vs.mu.Unlock()
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.view = View{0, "", ""}
	vs.primaryAck = 0
	vs.ackTick = 0
	vs.backupTick = 0

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me);
	if e != nil {
		log.Fatal("listen error: ", e);
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
