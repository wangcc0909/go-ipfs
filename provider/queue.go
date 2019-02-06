package provider

import (
	"context"
	"gx/ipfs/QmR8BauakNcBa3RbE4nbQu76PDiJgoQgz8AJdhJuiU4TAw/go-cid"
	ds "gx/ipfs/Qmf4xQhNomPNhrtZc67qSnfJSjxjXs9LWvknJtSXwimPrM/go-datastore"
	"gx/ipfs/Qmf4xQhNomPNhrtZc67qSnfJSjxjXs9LWvknJtSXwimPrM/go-datastore/query"
	"math"
	"strconv"
	"strings"
	"sync"
)

// Entry allows for the durability in the queue. When a cid is dequeued it is
// not removed from the datastore until you call Complete() on the entry you
// receive.
type Entry struct {
	cid cid.Cid
	key ds.Key
	datastore ds.Datastore
}

func (e *Entry) Complete() error {
	return e.datastore.Delete(e.key)
}

// Queue provides a durable, FIFO interface to the datastore for storing cids
//
// Durability just means that cids in the process of being provided when a
// crash or shutdown occurs will still be in the queue when the node is
// brought back online.
type Queue struct {
	// used to differentiate queues in datastore
	// e.g. provider vs reprovider
	name string

	ctx context.Context

	tail uint64
	head uint64

	lock sync.Mutex
	datastore ds.Datastore

	dequeue chan *Entry
	notEmpty chan struct{}
}

func NewQueue(name string, ctx context.Context, datastore ds.Datastore) (*Queue, error) {
	head, tail, err := getQueueHeadTail(name, ctx, datastore)
	if err != nil {
		return nil, err
	}
	q := &Queue{
		name: name,
		ctx: ctx,
		head: head,
		tail: tail,
		lock: sync.Mutex{},
		datastore: datastore,
		dequeue: make(chan *Entry),
		notEmpty: make(chan struct{}),
	}
	q.run()
	return q, nil
}

// Put a cid in the queue
func (q *Queue) Enqueue(cid cid.Cid) error {
	q.lock.Lock()
	defer q.lock.Unlock()

	wasEmpty := q.IsEmpty()

	nextKey := q.queueKey(q.tail)

	if err := q.datastore.Put(nextKey, cid.Bytes()); err != nil {
		return err
	}

	q.tail++

	if wasEmpty {
		select {
		case q.notEmpty <- struct{}{}:
		case <-q.ctx.Done():
		}
	}

	return nil
}

// Remove an entry from the queue.
func (q *Queue) Dequeue() <-chan *Entry {
	return q.dequeue
}

func (q *Queue) IsEmpty() bool {
	return (q.tail - q.head) == 0
}

// dequeue items when the dequeue channel is available to
// be written to
func (q *Queue) run() {
	go func() {
		for {
			select {
			case <-q.ctx.Done():
				return
			default:
			}
			if q.IsEmpty() {
				select {
				case <-q.ctx.Done():
					return
				// wait for a notEmpty message
				case <-q.notEmpty:
				}
			}

			entry, err := q.next()
			if err != nil {
				log.Warningf("Error Dequeue()-ing: %s, %s", entry, err)
				continue
			}

			select {
			case <-q.ctx.Done():
				return
			case q.dequeue <- entry:
			}
		}
	}()
}

// Find the next item in the queue, crawl forward if an entry is not
// found in the next spot.
func (q *Queue) next() (*Entry, error) {
	q.lock.Lock()
	defer q.lock.Unlock()

	var nextKey ds.Key
	var value []byte
	var err error
	for {
		// TODO: if head >= tail, then return nil, empty queue error
		select {
		case <-q.ctx.Done():
			return nil, nil
		default:
		}
		nextKey = q.queueKey(q.head)
		value, err = q.datastore.Get(nextKey)
		if err == ds.ErrNotFound {
			q.head++
			continue
		} else if err != nil {
			return nil, err
		} else {
			break
		}
	}

	id, err := cid.Parse(value)
	if err != nil {
		return nil, err
	}

	entry := &Entry {
		cid: id,
		key: nextKey,
		datastore: q.datastore,
	}

	q.head++

	return entry, nil
}

func (q *Queue) queueKey(id uint64) ds.Key {
	return ds.NewKey(queuePrefix(q.name) + strconv.FormatUint(id, 10))
}

func queuePrefix(name string) string {
	return "/" + name + "/queue/"
}

// crawl over the queue entries to find the head and tail
func getQueueHeadTail(name string, ctx context.Context, datastore ds.Datastore) (uint64, uint64, error) {
	query := query.Query{Prefix: queuePrefix(name)}
	results, err := datastore.Query(query)
	if err != nil {
		return 0, 0, err
	}

	var tail uint64 = 0
	var head uint64 = math.MaxUint64
	for entry := range results.Next() {
		select {
		case <-ctx.Done():
			return 0, 0, nil
		default:
		}
		keyId := strings.TrimPrefix(entry.Key, queuePrefix(name))
		id, err := strconv.ParseUint(keyId, 10, 64)
		if err != nil {
			return 0, 0, err
		}

		if id < head {
			head = id
		}

		if (id+1) > tail {
			tail = (id+1)
		}
	}
	if head == math.MaxUint64 {
		head = 0
	}

	return head, tail, nil
}
