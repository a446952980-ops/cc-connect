package qq

import (
	"context"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultSendIntervalMinMS  = 1000
	defaultSendIntervalMaxMS  = 2000
	defaultFinalReplyMaxChars = 120
)

type jobKind int

const (
	// kindMessage is a visible message; consecutive messages in the same chat
	// are spaced by a random interval so replies arrive at a human pace.
	kindMessage jobKind = iota
	// kindAction is a non-message QQ action (reaction, poke, typing status).
	// It runs in queue order without an added delay.
	kindAction
)

type outJob struct {
	kind      jobKind
	fn        func() error
	done      chan error
	cancelled atomic.Bool
}

// chatQueue serializes every outbound QQ operation of one chat (group or
// private) in FIFO order, so side messages, the final reply, reactions and
// pokes keep the order in which they were requested.
type chatQueue struct {
	p       *Platform
	mu      sync.Mutex
	jobs    []*outJob
	running bool
	lastMsg time.Time
}

// queueFor returns the queue for a chat key such as "g:123" or "u:456".
func (p *Platform) queueFor(key string) *chatQueue {
	if q, ok := p.queues.Load(key); ok {
		return q.(*chatQueue)
	}
	q, _ := p.queues.LoadOrStore(key, &chatQueue{p: p})
	return q.(*chatQueue)
}

// submit enqueues fn and blocks until it has run (or ctx is done first), so
// callers such as `cc-connect send` return only once the message was sent.
func (q *chatQueue) submit(ctx context.Context, kind jobKind, fn func() error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	job := &outJob{kind: kind, fn: fn, done: make(chan error, 1)}

	q.mu.Lock()
	q.jobs = append(q.jobs, job)
	if !q.running {
		q.running = true
		go q.run()
	}
	q.mu.Unlock()

	select {
	case err := <-job.done:
		return err
	case <-ctx.Done():
		job.cancelled.Store(true)
		return ctx.Err()
	}
}

func (q *chatQueue) run() {
	for {
		q.mu.Lock()
		if len(q.jobs) == 0 {
			q.running = false
			q.mu.Unlock()
			return
		}
		job := q.jobs[0]
		q.jobs = q.jobs[1:]
		q.mu.Unlock()

		if job.cancelled.Load() {
			job.done <- context.Canceled
			continue
		}
		if job.kind == kindMessage {
			q.waitGap()
		}
		err := job.fn()
		if job.kind == kindMessage {
			q.lastMsg = time.Now()
		}
		job.done <- err
	}
}

// waitGap sleeps until a random interval has passed since the previous
// message of this chat. The first message after an idle period is not delayed.
func (q *chatQueue) waitGap() {
	if q.lastMsg.IsZero() {
		return
	}
	if wait := q.p.randomGap() - time.Since(q.lastMsg); wait > 0 {
		time.Sleep(wait)
	}
}

// randomGap picks the spacing between two messages, uniformly in
// [sendIntervalMin, sendIntervalMax]. Zero values disable spacing.
func (p *Platform) randomGap() time.Duration {
	lo, hi := p.sendIntervalMin, p.sendIntervalMax
	if hi <= 0 {
		return 0
	}
	if hi <= lo {
		return hi
	}
	return lo + time.Duration(rand.Int64N(int64(hi-lo)+1))
}

// chatKey identifies the queue a reply context belongs to.
func chatKey(rctx *replyContext) string {
	if rctx.messageType == "group" {
		return "g:" + strconv.FormatInt(rctx.groupID, 10)
	}
	return "u:" + strconv.FormatInt(rctx.userID, 10)
}
