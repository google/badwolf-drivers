// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package bwbolt contains the implementation of the BadWolf driver using
// BoltDB.
package bwbolt

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/literal"
	"github.com/google/badwolf/triple/node"
	"github.com/google/badwolf/triple/predicate"
	"go.etcd.io/bbolt"
)

// driver implements BadWolf storage.Store for a fully compliant driver.
type driver struct {
	path string
	db   *bbolt.DB
	lb   literal.Builder
}

// graphBucket contains the name of the bucket containing the graphs.
const graphBucket = "GRAPHS"

// New create a new BadWolf driver using BoltDB as a storage driver.
func New(path string, lb literal.Builder, timeOut time.Duration, readOnly bool) (storage.Store, *bbolt.DB, error) {
	// Bolt open options.
	opts := &bbolt.Options{
		Timeout:  timeOut,
		ReadOnly: readOnly,
	}
	// Open the DB.
	db, err := bbolt.Open(path, 0600, opts)
	if err != nil {
		return nil, nil, fmt.Errorf("bwbolt driver initialization failure for file %q %v", path, err)
	}
	// Create the graph bucket if it does not exist.
	db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(graphBucket))
		if err != nil {
			return fmt.Errorf("driver initialization failed to create bucket %s for path %s with error %v", graphBucket, path, err)
		}
		return nil
	})
	// Return the initilized driver.
	return &driver{
		path: path,
		db:   db,
		lb:   lb,
	}, db, nil
}

// Name returns the ID of the backend being used.
func (d *driver) Name(ctx context.Context) string {
	return fmt.Sprintf("bwbolt/%s", d.path)
}

// Version returns the version of the driver implementation.
func (d *driver) Version(ctx context.Context) string {
	return "HEAD"
}

// NewGraph creates a new graph. Creating an already existing graph
// should return an error.
func (d *driver) NewGraph(ctx context.Context, id string) (storage.Graph, error) {
	err := d.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(graphBucket))
		if b == nil {
			return fmt.Errorf("inconsistent driver failed to retrieve top bucket %q", graphBucket)
		}
		_, err := b.CreateBucket([]byte(id))
		if err != nil {
			return fmt.Errorf("failed to create new graph %q with error %v", id, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &graph{
		id: id,
		db: d.db,
		lb: d.lb,
	}, nil
}

// Graph returns an existing graph if available. Getting a non existing
// graph should return an error.
func (d *driver) Graph(ctx context.Context, id string) (storage.Graph, error) {
	err := d.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(graphBucket))
		if b == nil {
			return fmt.Errorf("inconsistent driver failed to retrieve top bucket %q", graphBucket)
		}
		g := b.Bucket([]byte(id))
		if g == nil {
			return fmt.Errorf("graph %q does not exist", id)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &graph{
		id: id,
		db: d.db,
		lb: d.lb,
	}, nil
}

// DeleteGraph deletes an existing graph. Deleting a non existing graph
// should return an error.
func (d *driver) DeleteGraph(ctx context.Context, id string) error {
	return d.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(graphBucket))
		if b == nil {
			return fmt.Errorf("inconsistent driver failed to retrieve top bucket %q", graphBucket)
		}
		err := b.DeleteBucket([]byte(id))
		if err != nil {
			return err
		}
		return nil
	})
}

// GraphNames returns the current available graph names in the store.
func (d *driver) GraphNames(ctx context.Context, names chan<- string) error {
	defer close(names)
	return d.db.View(func(tx *bbolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte(graphBucket))
		if b == nil {
			return fmt.Errorf("inconsistent driver failed to retrieve top bucket %q", graphBucket)
		}
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			names <- string(k)
		}
		return nil
	})
}

// graph implements BadWolf storage.Graph for a fully compliant driver.
type graph struct {
	id string
	db *bbolt.DB
	lb literal.Builder
}

// ID returns the id for this graph.
func (g *graph) ID(ctx context.Context) string {
	return g.id
}

// indexUpdate contains the update to perform to a given index.
type indexUpdate struct {
	idx   string
	key   []byte
	value []byte
}

const (
	idxSPO = "SPO"
	idxSOP = "SOP"
	idxPOS = "POS"
	idxOPS = "OPS"
)

// Given a triple, returns the updates to perform to the indices.
func (g *graph) tripleToIndexUpdate(t *triple.Triple) []*indexUpdate {
	var updates []*indexUpdate
	s, p, o := t.Subject().String(), t.Predicate().String(), t.Object().String()
	tt := []byte(t.String())

	updates = append(updates,
		&indexUpdate{
			idx:   idxSPO,
			key:   []byte(s),
			value: nil,
		},
		&indexUpdate{
			idx:   idxSPO,
			key:   []byte(s + "\t" + p),
			value: nil,
		},
		&indexUpdate{
			idx:   idxSPO,
			key:   tt,
			value: tt,
		},
		&indexUpdate{
			idx:   idxSOP,
			key:   []byte(s),
			value: nil,
		},
		&indexUpdate{
			idx:   idxSOP,
			key:   []byte(s + "\t" + o),
			value: nil,
		},
		&indexUpdate{
			idx:   idxSOP,
			key:   []byte(s + "\t" + o + "\t" + p),
			value: tt,
		},
		&indexUpdate{
			idx:   idxPOS,
			key:   []byte(p),
			value: nil,
		},
		&indexUpdate{
			idx:   idxPOS,
			key:   []byte(p + "\t" + o),
			value: nil,
		},
		&indexUpdate{
			idx:   idxPOS,
			key:   []byte(p + "\t" + o + "\t" + s),
			value: tt,
		},
		&indexUpdate{
			idx:   idxOPS,
			key:   []byte(o),
			value: nil,
		},
		&indexUpdate{
			idx:   idxOPS,
			key:   []byte(o + "\t" + p),
			value: nil,
		},
		&indexUpdate{
			idx:   idxOPS,
			key:   []byte(o + "\t" + p + "\t" + s),
			value: tt,
		})

	return updates
}

// AddTriples adds the triples to the storage. Adding a triple that already
// exists should not fail. Efficiently resolving all the operations below
// require proper indexing. This driver provides the follow indices:
//
//   * SPO: Textual representation of the triple to allow range queries.
//   * SOP: Conbination to allow efficient query of S + P queries.
//   * POS: Conbination to allow efficient query of P + O queries.
//   * OPS: Conbination to allow efficient query of O + P queries.
//
// The GUID index containst the fully serialized triple. The other indices
// only contains as a value the GUID of the triple.
func (g *graph) AddTriples(ctx context.Context, ts []*triple.Triple) error {
	return g.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(graphBucket))
		if b == nil {
			return fmt.Errorf("inconsistent driver failed to retrieve top bucket %q", graphBucket)
		}
		gb := b.Bucket([]byte(g.id))
		if gb == nil {
			return fmt.Errorf("graph %q does not exist", g.id)
		}
		for _, t := range ts {
			for _, iu := range g.tripleToIndexUpdate(t) {
				b, err := gb.CreateBucketIfNotExists([]byte(iu.idx))
				if err != nil {
					return fmt.Errorf("failed to create bucket %s for graph %s with error %v", iu.idx, g.id, err)
				}
				err = b.Put(iu.key, iu.value)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// RemoveTriples removes the trilpes from the storage. Removing triples that
// are not present on the store should not fail.
func (g *graph) RemoveTriples(ctx context.Context, ts []*triple.Triple) error {
	return g.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(graphBucket))
		if b == nil {
			return fmt.Errorf("inconsistent driver failed to retrieve top bucket %q", graphBucket)
		}
		gb := b.Bucket([]byte(g.id))
		if gb == nil {
			return fmt.Errorf("graph %q does not exist", g.id)
		}
		for _, t := range ts {
			for _, iu := range g.tripleToIndexUpdate(t) {
				b, err := gb.CreateBucketIfNotExists([]byte(iu.idx))
				if err != nil {
					return fmt.Errorf("failed to create bucket %s for graph %s with error %v", iu.idx, g.id, err)
				}
				b.Delete(iu.key)
				if err != nil {
					if b.Get(iu.key) != nil {
						return err
					}
				}
			}
		}
		return nil
	})
}

// shouldAccept returns is the triple should be accepted
func (g *graph) shouldAccept(cnt int, t *triple.Triple, lo *storage.LookupOptions) bool {
	if lo.MaxElements > 0 && lo.MaxElements < cnt {
		return false
	}
	p := t.Predicate()
	if p.Type() == predicate.Temporal {
		if lo.LowerAnchor != nil {
			ta, err := t.Predicate().TimeAnchor()
			if err != nil {
				panic(fmt.Errorf("should have never failed to retrieve time anchor from triple %s with error %v", t.String(), err))
			}
			if lo.LowerAnchor.After(*ta) {
				return false
			}
		}
		if lo.UpperAnchor != nil {
			ta, err := t.Predicate().TimeAnchor()
			if err != nil {
				panic(fmt.Errorf("should have never failed to retrieve time anchor from triple %s with error %v", t.String(), err))
			}
			if lo.UpperAnchor.Before(*ta) {
				return false
			}
		}
	}
	return true
}

// Objects pushes to the provided channel the objects for the given object and
// predicate. The function does not return immediately but spawns a goroutine
// to satisfy elements in the channel.
func (g *graph) Objects(ctx context.Context, s *node.Node, p *predicate.Predicate, lo *storage.LookupOptions, objs chan<- *triple.Object) error {
	defer close(objs)
	return g.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(graphBucket))
		if b == nil {
			return fmt.Errorf("inconsistent driver failed to retrieve top bucket %q", graphBucket)
		}
		gb := b.Bucket([]byte(g.id))
		if gb == nil {
			return fmt.Errorf("graph %q does not exist", g.id)
		}
		spo := gb.Bucket([]byte(idxSPO))
		if spo == nil {
			return fmt.Errorf("failed to load bucket SPO for graph %s", g.id)
		}
		cnt, c, prefix := 0, spo.Cursor(), []byte(s.String()+"\t"+p.String())
		for k, v := c.Seek(prefix); bytes.HasPrefix(k, prefix); k, v = c.Next() {
			if reflect.DeepEqual(v, []byte{}) {
				continue
			}
			t, err := triple.Parse(string(v), g.lb)
			if err != nil {
				return fmt.Errorf("corrupt index SPO failing to parse %q with error %v", string(v), err)
			}
			if g.shouldAccept(cnt, t, lo) {
				objs <- t.Object()
				cnt++
			}
		}
		return nil
	})
}

// Subjects pushes to the provided channel the subjects for the give predicate
// and object. The function does not return immediately but spawns a
// goroutine to satisfy elements in the channel.
func (g *graph) Subjects(ctx context.Context, p *predicate.Predicate, o *triple.Object, lo *storage.LookupOptions, subs chan<- *node.Node) error {
	defer close(subs)
	return g.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(graphBucket))
		if b == nil {
			return fmt.Errorf("inconsistent driver failed to retrieve top bucket %q", graphBucket)
		}
		gb := b.Bucket([]byte(g.id))
		if gb == nil {
			return fmt.Errorf("graph %q does not exist", g.id)
		}
		pos := gb.Bucket([]byte(idxPOS))
		if pos == nil {
			return fmt.Errorf("failed to load bucket POS for graph %s", g.id)
		}
		cnt, c, prefix := 0, pos.Cursor(), []byte(p.String()+"\t"+o.String())
		for k, v := c.Seek(prefix); bytes.HasPrefix(k, prefix); k, v = c.Next() {
			if reflect.DeepEqual(v, []byte{}) {
				continue
			}
			t, err := triple.Parse(string(v), g.lb)
			if err != nil {
				return fmt.Errorf("corrupt index POS failing to parse %q with error %v", string(v), err)
			}
			if g.shouldAccept(cnt, t, lo) {
				subs <- t.Subject()
				cnt++
			}
		}
		return nil
	})
}

// PredicatesForSubject pushes to the provided channel all the predicates
// known for the given subject. The function does not return immediately but
// spawns a goroutine to satisfy elements in the channel.
func (g *graph) PredicatesForSubject(ctx context.Context, s *node.Node, lo *storage.LookupOptions, prds chan<- *predicate.Predicate) error {
	defer close(prds)
	return g.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(graphBucket))
		if b == nil {
			return fmt.Errorf("inconsistent driver failed to retrieve top bucket %q", graphBucket)
		}
		gb := b.Bucket([]byte(g.id))
		if gb == nil {
			return fmt.Errorf("graph %q does not exist", g.id)
		}
		spo := gb.Bucket([]byte(idxSPO))
		if spo == nil {
			return fmt.Errorf("failed to load bucket SPO for graph %s", g.id)
		}
		cnt, c, prefix := 0, spo.Cursor(), []byte(s.String())
		for k, v := c.Seek(prefix); bytes.HasPrefix(k, prefix); k, v = c.Next() {
			if reflect.DeepEqual(v, []byte{}) {
				continue
			}
			t, err := triple.Parse(string(v), g.lb)
			if err != nil {
				return fmt.Errorf("corrupt index SPO failing to parse %q with error %v", string(v), err)
			}
			if g.shouldAccept(cnt, t, lo) {
				prds <- t.Predicate()
				cnt++
			}
		}
		return nil
	})
}

// PredicatesForObject pushes to the provided channel all the predicates known
// for the given object. The function returns immediately and spawns a go
// routine to satisfy elements in the channel.
func (g *graph) PredicatesForObject(ctx context.Context, o *triple.Object, lo *storage.LookupOptions, prds chan<- *predicate.Predicate) error {
	defer close(prds)
	return g.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(graphBucket))
		if b == nil {
			return fmt.Errorf("inconsistent driver failed to retrieve top bucket %q", graphBucket)
		}
		gb := b.Bucket([]byte(g.id))
		if gb == nil {
			return fmt.Errorf("graph %q does not exist", g.id)
		}
		ops := gb.Bucket([]byte(idxOPS))
		if ops == nil {
			return fmt.Errorf("failed to load bucket OPS for graph %s", g.id)
		}
		cnt, c, prefix := 0, ops.Cursor(), []byte(o.String())
		for k, v := c.Seek(prefix); bytes.HasPrefix(k, prefix); k, v = c.Next() {
			if reflect.DeepEqual(v, []byte{}) {
				continue
			}
			t, err := triple.Parse(string(v), g.lb)
			if err != nil {
				return fmt.Errorf("corrupt index OPS failing to parse %q with error %v", string(v), err)
			}
			if g.shouldAccept(cnt, t, lo) {
				prds <- t.Predicate()
				cnt++
			}
		}
		return nil
	})
}

// PredicatesForSubjectAndObject pushes to the provided channel all predicates
// available for the given subject and object. The function does not return
// immediately but spawns a goroutine to satisfy elements in the channel.
func (g *graph) PredicatesForSubjectAndObject(ctx context.Context, s *node.Node, o *triple.Object, lo *storage.LookupOptions, prds chan<- *predicate.Predicate) error {
	defer close(prds)
	return g.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(graphBucket))
		if b == nil {
			return fmt.Errorf("inconsistent driver failed to retrieve top bucket %q", graphBucket)
		}
		gb := b.Bucket([]byte(g.id))
		if gb == nil {
			return fmt.Errorf("graph %q does not exist", g.id)
		}
		sop := gb.Bucket([]byte(idxSOP))
		if sop == nil {
			return fmt.Errorf("failed to load bucket SOP for graph %s", g.id)
		}
		cnt, c, prefix := 0, sop.Cursor(), []byte(s.String()+"\t"+o.String())
		for k, v := c.Seek(prefix); bytes.HasPrefix(k, prefix); k, v = c.Next() {
			if reflect.DeepEqual(v, []byte{}) {
				continue
			}
			t, err := triple.Parse(string(v), g.lb)
			if err != nil {
				return fmt.Errorf("corrupt index SOP failing to parse %q with error %v", string(v), err)
			}
			if g.shouldAccept(cnt, t, lo) {
				prds <- t.Predicate()
				cnt++
			}
		}
		return nil
	})
}

// TriplesForSubject pushes to the provided channel all triples available for
// the given subect. The function does not return immediately but spawns a
// goroutine to satisfy elements in the channel.
func (g *graph) TriplesForSubject(ctx context.Context, s *node.Node, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	defer close(trpls)
	return g.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(graphBucket))
		if b == nil {
			return fmt.Errorf("inconsistent driver failed to retrieve top bucket %q", graphBucket)
		}
		gb := b.Bucket([]byte(g.id))
		if gb == nil {
			return fmt.Errorf("graph %q does not exist", g.id)
		}
		spo := gb.Bucket([]byte(idxSPO))
		if spo == nil {
			return fmt.Errorf("failed to load bucket SPO for graph %s", g.id)
		}
		cnt, c, prefix := 0, spo.Cursor(), []byte(s.String())
		for k, v := c.Seek(prefix); bytes.HasPrefix(k, prefix); k, v = c.Next() {
			if reflect.DeepEqual(v, []byte{}) {
				continue
			}
			t, err := triple.Parse(string(v), g.lb)
			if err != nil {
				return fmt.Errorf("corrupt index SPO failing to parse %q with error %v", string(v), err)
			}
			if g.shouldAccept(cnt, t, lo) {
				trpls <- t
				cnt++
			}
		}
		return nil
	})
}

// TriplesForPredicate pushes to the provided channel all triples available
// for the given predicate.The function does not return immediately but spawns
// a goroutine to satisfy elements in the channel.
func (g *graph) TriplesForPredicate(ctx context.Context, p *predicate.Predicate, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	defer close(trpls)
	return g.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(graphBucket))
		if b == nil {
			return fmt.Errorf("inconsistent driver failed to retrieve top bucket %q", graphBucket)
		}
		gb := b.Bucket([]byte(g.id))
		if gb == nil {
			return fmt.Errorf("graph %q does not exist", g.id)
		}
		pos := gb.Bucket([]byte(idxPOS))
		if pos == nil {
			return fmt.Errorf("failed to load bucket POS for graph %s", g.id)
		}
		cnt, c, prefix := 0, pos.Cursor(), []byte(p.String())
		for k, v := c.Seek(prefix); bytes.HasPrefix(k, prefix); k, v = c.Next() {
			if reflect.DeepEqual(v, []byte{}) {
				continue
			}
			t, err := triple.Parse(string(v), g.lb)
			if err != nil {
				return fmt.Errorf("corrupt index POS failing to parse %q with error %v", string(v), err)
			}
			if g.shouldAccept(cnt, t, lo) {
				trpls <- t
				cnt++
			}
		}
		return nil
	})
}

// TriplesForObject pushes to the provided channel all triples available for
// the given object. The function does not return immediately but spawns a
// goroutine to satisfy elements in the channel.
func (g *graph) TriplesForObject(ctx context.Context, o *triple.Object, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	defer close(trpls)
	return g.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(graphBucket))
		if b == nil {
			return fmt.Errorf("inconsistent driver failed to retrieve top bucket %q", graphBucket)
		}
		gb := b.Bucket([]byte(g.id))
		if gb == nil {
			return fmt.Errorf("graph %q does not exist", g.id)
		}
		pos := gb.Bucket([]byte(idxOPS))
		if pos == nil {
			return fmt.Errorf("failed to load bucket POS for graph %s", g.id)
		}
		cnt, c, prefix := 0, pos.Cursor(), []byte(o.String())
		for k, v := c.Seek(prefix); bytes.HasPrefix(k, prefix); k, v = c.Next() {
			if reflect.DeepEqual(v, []byte{}) {
				continue
			}
			t, err := triple.Parse(string(v), g.lb)
			if err != nil {
				return fmt.Errorf("corrupt index POS failing to parse %q with error %v", string(v), err)
			}
			if g.shouldAccept(cnt, t, lo) {
				trpls <- t
				cnt++
			}
		}
		return nil
	})
}

// TriplesForSubjectAndPredicate pushes to the provided channel all triples
// available for the given subject and predicate. The function does not return
// immediately but spawns a goroutine to satisfy elements in the channel.
func (g *graph) TriplesForSubjectAndPredicate(ctx context.Context, s *node.Node, p *predicate.Predicate, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	defer close(trpls)
	return g.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(graphBucket))
		if b == nil {
			return fmt.Errorf("inconsistent driver failed to retrieve top bucket %q", graphBucket)
		}
		gb := b.Bucket([]byte(g.id))
		if gb == nil {
			return fmt.Errorf("graph %q does not exist", g.id)
		}
		spo := gb.Bucket([]byte(idxSPO))
		if spo == nil {
			return fmt.Errorf("failed to load bucket SPO for graph %s", g.id)
		}
		cnt, c, prefix := 0, spo.Cursor(), []byte(s.String()+"\t"+p.String())
		for k, v := c.Seek(prefix); bytes.HasPrefix(k, prefix); k, v = c.Next() {
			if reflect.DeepEqual(v, []byte{}) {
				continue
			}
			t, err := triple.Parse(string(v), g.lb)
			if err != nil {
				return fmt.Errorf("corrupt index SPO failing to parse %q with error %v", string(v), err)
			}
			if g.shouldAccept(cnt, t, lo) {
				trpls <- t
				cnt++
			}
		}
		return nil
	})
}

// TriplesForPredicateAndObject pushes to the provided channel all triples
// available for the given predicate and object. The function does not return
// immediately but spawns a goroutine to satisfy elements in the channel.
func (g *graph) TriplesForPredicateAndObject(ctx context.Context, p *predicate.Predicate, o *triple.Object, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	defer close(trpls)
	return g.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(graphBucket))
		if b == nil {
			return fmt.Errorf("inconsistent driver failed to retrieve top bucket %q", graphBucket)
		}
		gb := b.Bucket([]byte(g.id))
		if gb == nil {
			return fmt.Errorf("graph %q does not exist", g.id)
		}
		pos := gb.Bucket([]byte(idxPOS))
		if pos == nil {
			return fmt.Errorf("failed to load bucket SPO for graph %s", g.id)
		}
		cnt, c, prefix := 0, pos.Cursor(), []byte(p.String()+"\t"+o.String())
		for k, v := c.Seek(prefix); bytes.HasPrefix(k, prefix); k, v = c.Next() {
			if reflect.DeepEqual(v, []byte{}) {
				continue
			}
			t, err := triple.Parse(string(v), g.lb)
			if err != nil {
				return fmt.Errorf("corrupt index POS failing to parse %q with error %v", string(v), err)
			}
			if g.shouldAccept(cnt, t, lo) {
				trpls <- t
				cnt++
			}
		}
		return nil
	})
}

// Exist checks if the provided triple exists on the store.
func (g *graph) Exist(ctx context.Context, t *triple.Triple) (bool, error) {
	res := false

	err := g.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(graphBucket))
		if b == nil {
			return fmt.Errorf("inconsistent driver failed to retrieve top bucket %q", graphBucket)
		}
		gb := b.Bucket([]byte(g.id))
		if gb == nil {
			return fmt.Errorf("graph %q does not exist", g.id)
		}
		spo := gb.Bucket([]byte(idxSPO))
		if spo == nil {
			return fmt.Errorf("failed to load bucket SPO for graph %s", g.id)
		}
		if spo.Get([]byte(t.String())) != nil {
			res = true
		}
		return nil
	})

	return res, err
}

// Triples pushes to the provided channel all available triples in the graph.
// The function does not return immediately but spawns a goroutine to satisfy
// elements in the channel.
func (g *graph) Triples(ctx context.Context, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	defer close(trpls)
	return g.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(graphBucket))
		if b == nil {
			return fmt.Errorf("inconsistent driver failed to retrieve top bucket %q", graphBucket)
		}
		gb := b.Bucket([]byte(g.id))
		if gb == nil {
			return fmt.Errorf("graph %q does not exist", g.id)
		}
		spo := gb.Bucket([]byte(idxSPO))
		if spo == nil {
			return fmt.Errorf("failed to load bucket SPO for graph %s", g.id)
		}
		cnt, c := 0, spo.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if reflect.DeepEqual(v, []byte{}) {
				continue
			}
			t, err := triple.Parse(string(v), g.lb)
			if err != nil {
				return fmt.Errorf("corrupt index SPO failing to parse %q with error %v", string(v), err)
			}
			if g.shouldAccept(cnt, t, lo) {
				trpls <- t
				cnt++
			}
		}
		return nil
	})
}
