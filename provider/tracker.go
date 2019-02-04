package provider

import (
	"context"
	"gx/ipfs/QmR8BauakNcBa3RbE4nbQu76PDiJgoQgz8AJdhJuiU4TAw/go-cid"
	ds "gx/ipfs/Qmf4xQhNomPNhrtZc67qSnfJSjxjXs9LWvknJtSXwimPrM/go-datastore"
	"gx/ipfs/Qmf4xQhNomPNhrtZc67qSnfJSjxjXs9LWvknJtSXwimPrM/go-datastore/query"
)

const providerTrackingPrefix = "/provider/tracking/"

type Tracker struct {
	datastore ds.Datastore
}

func NewTracker(datastore ds.Datastore) *Tracker {
	return &Tracker{
		datastore: datastore,
	}
}

func (t *Tracker) IsTracking(cid cid.Cid) (bool, error) {
	return t.datastore.Has(providerTrackingKey(cid))
}

func (t *Tracker) Track(cid cid.Cid) error {
	return t.datastore.Put(providerTrackingKey(cid), cid.Bytes())
}

func (t *Tracker) Untrack(cid cid.Cid) error {
	return t.datastore.Delete(providerTrackingKey(cid))
}

func (t *Tracker) Tracking(ctx context.Context) (<-chan cid.Cid, error) {
	q := query.Query{Prefix: providerTrackingPrefix}
	results, err := t.datastore.Query(q)
	if err != nil {
		return nil, err
	}
	cids := make(chan cid.Cid)
	go func() {
		defer close(cids);
		for result := range results.Next() {
			key, err := cid.Parse(result.Value)
			if err != nil {
				log.Warningf("unable to parse tracked cid: %s", err)
			}
			select {
			case <-ctx.Done():
				return
			case cids <- key:
			}
		}
	}()
	return cids, nil
}

func providerTrackingKey(cid cid.Cid) ds.Key {
	return ds.NewKey(providerTrackingPrefix + cid.String())
}
