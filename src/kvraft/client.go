package kvraft

import (
	"crypto/rand"
	"math/big"

	"../labrpc"
)

type Clerk struct {
	servers      []*labrpc.ClientEnd
	clientID     int64
	opIndex      int64
	leaderServer int
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientID = nrand()
	ck.leaderServer = -1
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:      key,
		ClientID: ck.clientID,
		OpIndex:  nrand(),
	}
	reply := GetReply{}
	for {
		if ck.leaderServer == -1 {
			for i := 0; i < len(ck.servers); i++ {
				ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
				if ok && reply.Success {
					ck.leaderServer = i
					return reply.Value
				}
			}
		} else {
			ok := ck.servers[ck.leaderServer].Call("KVServer.Get", &args, &reply)
			if ok && reply.Success {
				return reply.Value
			}
			ck.leaderServer = -1
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		ClientID: ck.clientID,
		OpIndex:  nrand(),
		Op:       op,
		Key:      key,
		Value:    value,
	}
	reply := PutAppendReply{}
	for {
		if ck.leaderServer == -1 {
			for i := 0; i < len(ck.servers); i++ {
				ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
				if ok && reply.Success {
					ck.leaderServer = i
					return
				}
			}
		} else {
			ok := ck.servers[ck.leaderServer].Call("KVServer.PutAppend", &args, &reply)
			if ok && reply.Success {
				return
			}
			ck.leaderServer = -1
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PutOp)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, AppendOp)
}
