package provider

import (
	"gx/ipfs/QmR8BauakNcBa3RbE4nbQu76PDiJgoQgz8AJdhJuiU4TAw/go-cid"
	ds "gx/ipfs/Qmf4xQhNomPNhrtZc67qSnfJSjxjXs9LWvknJtSXwimPrM/go-datastore"
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

func providerTrackingKey(cid cid.Cid) ds.Key {
	return ds.NewKey(providerTrackingPrefix + cid.String())
}
