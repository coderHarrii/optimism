package sema

import (
	"context"
	"sync"

	"sync/atomic"

	"github.com/ethereum-optimism/optimism/op-service/safego"
)

// Lock is a semaphore lock; locking based on a 1-capacity go channel.
// This allows lock-attempts to abort on context cancellation.
// It is used like a standard Mutex, with the addition of a LockCtx function.
type Lock struct {
	locked atomic.Bool

	initOnce sync.Once

	// lockCh is used to gain the lock over the state, like a mutex, but with the ability to watch as channel.
	// The channel has a capacity of 1. To gain control, insert into the channel. To release control, empty it.
	lockCh chan struct{}

	_ safego.NoCopy
}

func (s *Lock) init() {
	s.lockCh = make(chan struct{}, 1)
}

// Lock acquires the lock, blocking until acquired.
func (s *Lock) Lock() {
	s.initOnce.Do(s.init)

	s.lockCh <- struct{}{}
	s.locked.Store(true)
}

// LockCtx tries to get a lock, but may abort with error if the provided ctx is canceled first.
func (s *Lock) LockCtx(ctx context.Context) error {
	s.initOnce.Do(s.init)

	select {
	case s.lockCh <- struct{}{}:
		s.locked.Store(true)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Unlock releases the lock. Unlock panics if the state was not locked.
func (s *Lock) Unlock() {
	if !s.locked.CompareAndSwap(true, false) {
		panic("cannot unlock already unlocked SemaLock")
	}
	// if it was locked, lockCh was initialized
	<-s.lockCh
}
