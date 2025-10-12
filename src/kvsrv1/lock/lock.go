package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck  kvtest.IKVClerk
	uid string
	key string
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.key = l
	lk.uid = kvtest.RandValue(8)
	return lk
}

func (lk *Lock) Acquire() {
	for {
		value, version, err := lk.ck.Get(lk.key)
		if value != "" && err != rpc.ErrNoKey {
			time.Sleep(2 * time.Second)
			continue
		}
		lk.ck.Put(lk.key, lk.uid, version)
		value, version, err = lk.ck.Get(lk.key)
		if value != lk.uid || err != rpc.OK {
			time.Sleep(2 * time.Second)
			continue
		}
		return

	}
}

func (lk *Lock) Release() {
	// Your code here
	value, version, err := lk.ck.Get(lk.key)
	if value != lk.uid || err == rpc.ErrNoKey {
		return
	}
	lk.ck.Put(lk.key, "", version)

}
