package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0
const PutOp = "Put"
const GetOp = "Get"
const AppendOp = "Append"
const IsNotLeader = "Is not the leader"
const TimeOut = "Timeout"

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op       string
	Key      string
	Value    string
	ClientID int64
	OpIndex  int64
}

type opResult struct {
	op       string
	clientID int64
	opIndex  int64
	value    string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate    int // snapshot if log grows this big
	storage         map[string]string
	clientResultMap map[int64]opResult
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	clientResult, ok := kv.clientResultMap[args.ClientID]
	kv.mu.Unlock()
	if ok && clientResult.opIndex == args.OpIndex {
		reply.Success = true
		reply.Value = clientResult.value
		return
	}
	op := Op{
		Op:       GetOp,
		Key:      args.Key,
		ClientID: args.ClientID,
		OpIndex:  args.OpIndex,
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Success = false
		reply.Err = IsNotLeader
		return
	}
	t0 := time.Now()
	for time.Since(t0).Seconds() < 2 {
		time.Sleep(10 * time.Millisecond)
		kv.mu.Lock()
		clientResult, ok := kv.clientResultMap[args.ClientID]
		kv.mu.Unlock()
		if ok && clientResult.opIndex == args.OpIndex {
			reply.Success = true
			reply.Value = clientResult.value
			return
		}
	}
	reply.Success = false
	reply.Err = TimeOut
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	clientResult, ok := kv.clientResultMap[args.ClientID]
	kv.mu.Unlock()
	if ok && clientResult.opIndex == args.OpIndex {
		reply.Success = true
		return
	}
	op := Op{
		Op:       args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientID: args.ClientID,
		OpIndex:  args.OpIndex,
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Success = false
		reply.Err = IsNotLeader
		return
	}
	t0 := time.Now()
	for time.Since(t0).Seconds() < 2 {
		time.Sleep(10 * time.Millisecond)
		kv.mu.Lock()
		clientResult, ok := kv.clientResultMap[args.ClientID]
		kv.mu.Unlock()
		if ok && clientResult.opIndex == args.OpIndex {
			reply.Success = true
			return
		}
	}
	reply.Success = false
	reply.Err = TimeOut
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) apply() {
	for {
		applyMsg := <-kv.applyCh
		kv.mu.Lock()
		op, ok := applyMsg.Command.(Op)
		if ok {
			clientResult, ok2 := kv.clientResultMap[op.ClientID]
			if !(ok2 && clientResult.opIndex == op.OpIndex) {
				switch op.Op {
				case GetOp:
					kv.clientResultMap[op.ClientID] = opResult{
						op:       GetOp,
						clientID: op.ClientID,
						opIndex:  op.OpIndex,
						value:    kv.storage[op.Key],
					}
				case PutOp:
					kv.storage[op.Key] = op.Value
					kv.clientResultMap[op.ClientID] = opResult{
						op:       PutOp,
						clientID: op.ClientID,
						opIndex:  op.OpIndex,
					}
				case AppendOp:
					kv.storage[op.Key] = kv.storage[op.Key] + op.Value
					kv.clientResultMap[op.ClientID] = opResult{
						op:       AppendOp,
						clientID: op.ClientID,
						opIndex:  op.OpIndex,
					}
				}
			}
		} else {
			fmt.Println("applyMsg.Command convertion failed.")
		}
		kv.mu.Unlock()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.storage = make(map[string]string)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.clientResultMap = make(map[int64]opResult)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.apply()
	// You may need initialization code here.

	return kv
}
