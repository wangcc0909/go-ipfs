package provider

import (
	"context"
	"gx/ipfs/QmR8BauakNcBa3RbE4nbQu76PDiJgoQgz8AJdhJuiU4TAw/go-cid"
	"gx/ipfs/QmZBH87CAPFHcc7cYmBqeSQ98zQ3SX9KUxiYgzPmLWNVKz/go-libp2p-routing"
	logging "gx/ipfs/QmcuXC5cxs79ro2cUuHs4HQ2bkDLJUYokwL8aivcX6HW3C/go-log"
	"sync"
	"time"
)

var (
	log = logging.Logger("provider")
)

const (
	provideOutgoingWorkerLimit = 512
	provideOutgoingTimeout     = 15 * time.Second
)

type Strategy func(context.Context, cid.Cid) <-chan cid.Cid

type Provider struct {
	ctx context.Context
	lock sync.Mutex

	// CIDs we are working on providing now
	outgoing chan cid.Cid
	// strategy for deciding which CIDs, given a CID, should be provided
	strategy Strategy
	// keeps track of which CIDs have been provided already
	tracker *Tracker
	// the CIDs for which provide announcements should be made
	queue *Queue
	// used to announce providing to the network
	contentRouting routing.ContentRouting
}

func NewProvider(ctx context.Context, strategy Strategy, tracker *Tracker, queue *Queue, contentRouting routing.ContentRouting) *Provider {
	return &Provider{
		ctx:            ctx,
		strategy:       strategy,
		tracker:        tracker,
		queue:          queue,
		contentRouting: contentRouting,
		lock:           sync.Mutex{},
		outgoing:       make(chan cid.Cid),
	}
}

// Start workers to handle provide requests.
func (p *Provider) Run() {
	go p.handlePopulateOutgoing()
	go p.handleOutgoing()
}

// Provider the given cid using specified strategy.
func (p *Provider) Provide(root cid.Cid) error {
	cids := p.strategy(p.ctx, root)

	for cid := range cids {
		isTracking, err := p.tracker.IsTracking(cid)
		if err != nil {
			return err
		}

		if !isTracking {
			p.lock.Lock()
			if err := p.queue.Enqueue(cid); err != nil {
				p.lock.Unlock()
				return err
			}
			p.lock.Unlock()
		}
	}

	return nil
}

func (p *Provider) Unprovide(cid cid.Cid) error {
	return p.tracker.Untrack(cid)
}

// Announce to the world that a block is provided.
func (p *Provider) announce(cid cid.Cid) error {
	ctx, cancel := context.WithTimeout(p.ctx, provideOutgoingTimeout)
	defer cancel()
	if err := p.contentRouting.Provide(ctx, cid, true); err != nil {
		log.Warning("Failed to provide key: %s", err)
		return err
	}
	return nil
}

// Move CIDs from the providing queue to the outgoing channel
func (p *Provider) handlePopulateOutgoing() {
	for {
		select {
		case <-p.ctx.Done():
			return
		default:
		}

		p.lock.Lock()
		if p.queue.IsEmpty() {
			p.lock.Unlock()
			time.Sleep(1 * time.Second)
			continue
		}
		// TODO: We should probably not actually Dequeue() right here, or at
		// least have a plan to replace the entry in the event that something
		// goes wrong or the process is killed
		key, err := p.queue.Dequeue()
		p.lock.Unlock()
		if err != nil {
			log.Warning("Unable to dequeue from providing queue: %s", err)
			continue
		}
		p.outgoing <- key
	}
}

// Handle all outgoing cids by providing (announcing) them
func (p *Provider) handleOutgoing() {
	for workers := 0; workers < provideOutgoingWorkerLimit; workers++ {
		go func() {
			for {
				select {
				case key := <-p.outgoing:
					isTracking, err := p.tracker.IsTracking(key)
					if err != nil {
						log.Warning("Unable to check provider tracking on outgoing: %s, %s", key, err)
						continue
					}
					if isTracking {
						continue
					}

					if err := p.announce(key); err != nil {
						log.Warning("Unable to announce providing: %s, %s", key, err)
						continue
					}

					p.tracker.Track(key)
				case <-p.ctx.Done():
					return
				}
			}
		}()
	}
}

