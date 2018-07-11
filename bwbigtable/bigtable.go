// Copyright 2018 Google Inc. All rights reserved.
//
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

// Package bwbigtable contains the implementation of storage.Store and
// storage.Graph interfaces to enable persistent storage on Bigtable.
// This is done by implementing the interfaces defined at
// https://github.com/google/badwolf/blob/master/storage/storage.go
package bwbigtable

import (
	"context"
	"fmt"
	"math"
	"strings"

	"cloud.google.com/go/bigtable"
	"github.com/google/badwolf-drivers/bwbigtable/keys"
	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/literal"
	"github.com/google/badwolf/triple/node"
	"github.com/google/badwolf/triple/predicate"
	"github.com/pborman/uuid"
)

const (
	version = "0.1.0-dev"
)

// store implements the storage.Store interface using Bigtable.
type store struct {
	spec                  *TableSpec
	table                 *bigtable.Table
	bqlChannelSize        int
	bulkTripleBuilderSize int
	bulkTripleOpSize      int
	literalBuilder        literal.Builder
}

// TableSpec contains the configuration for the table to use.
type TableSpec struct {
	// ProjectID contains the table to use.
	ProjectID string
	// InstanceID contains the instance.
	InstanceID string
	// TableID to use in the store.
	TableID string
}

// New returns a new store attached to the table specified using the
// project, instance, and table IDs.
func New(ctx context.Context, spec *TableSpec, bulkTripleOpSize, bulkTripleBuilderSize, bqlChannelSize int) (storage.Store, error) {
	cli, err := bigtable.NewClient(ctx, spec.ProjectID, spec.InstanceID)
	if err != nil {
		fmt.Errorf(
			"bigtable.NewClient(ctx, %q, %q) failed; %v",
			spec.ProjectID, spec.InstanceID, err)
	}
	tbl := cli.Open(spec.TableID)

	return &store{
		spec:                  spec,
		table:                 tbl,
		bqlChannelSize:        bqlChannelSize,
		bulkTripleBuilderSize: bulkTripleBuilderSize,
		bulkTripleOpSize:      bulkTripleOpSize,
		literalBuilder:        literal.NewBoundedBuilder(bulkTripleBuilderSize),
	}, nil
}

// Name returns the ID of the backend being used.
func (s *store) Name(ctx context.Context) string {
	return fmt.Sprintf("bigtable://%s:%s/%s",
		s.spec.ProjectID, s.spec.InstanceID, s.spec.TableID)
}

// Version returns the version of the driver implementation.
func (s *store) Version(ctx context.Context) string {
	return "bwbigtable@" + version
}

// NewGraph creates a new graph. Creating an already existing graph
// should return an error.
func (s *store) NewGraph(ctx context.Context, id string) (storage.Graph, error) {
	b, err := s.exist(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("exist failed: %v", err)
	}
	if b {
		return nil, fmt.Errorf("graph %q already exist", id)
	}

	k := keys.ForGraph(id)
	if err := s.persistBytes(ctx, k.Row, k.Column, k.Timestamp, []byte(id)); err != nil {
		return nil, fmt.Errorf("persistMessasge failed: %v", err)
	}
	return &graph{
		id:                    id,
		table:                 s.table,
		bqlChannelSize:        s.bqlChannelSize,
		bulkTripleBuilderSize: s.bulkTripleBuilderSize,
		bulkTripleOpSize:      s.bulkTripleOpSize,
		literalBuilder:        s.literalBuilder,
	}, nil
}

// Graph returns an existing graph if available. Getting a non existing
// graph should return an error.
func (s *store) Graph(ctx context.Context, id string) (storage.Graph, error) {
	b, err := s.exist(ctx, id)
	if err != nil {
		return nil, err
	}
	if !b {
		return nil, fmt.Errorf("graph %q does not exist", id)
	}
	return &graph{
		id:                    id,
		table:                 s.table,
		bqlChannelSize:        s.bqlChannelSize,
		bulkTripleBuilderSize: s.bulkTripleBuilderSize,
		bulkTripleOpSize:      s.bulkTripleOpSize,
		literalBuilder:        s.literalBuilder,
	}, nil
}

// DeleteGraph deletes an existing graph. Deleting a non existing graph
// should return an error.
func (s *store) DeleteGraph(ctx context.Context, id string) error {
	g, err := s.Graph(ctx, id)
	if err != nil {
		return err
	}
	// Remove all triples for the graph.
	tc := make(chan *triple.Triple, s.bqlChannelSize)
	go func() {
		if err := g.Triples(ctx, storage.DefaultLookup, tc); err != nil {
			return
		}
	}()
	var (
		errDelete error
		cnt       = s.bulkTripleOpSize
		trpls     []*triple.Triple
	)
	for t := range tc {
		if errDelete != nil {
			// Need to drain the channel to release resources.
			continue
		}

		if cnt == 0 {
			if err := g.RemoveTriples(ctx, trpls); err != nil && errDelete == nil {
				errDelete = err
			}
			trpls = []*triple.Triple{}
		}
		trpls = append(trpls, t)
	}
	if errDelete != nil {
		return errDelete
	}
	if err := g.RemoveTriples(ctx, trpls); err != nil {
		return err
	}
	// Remove the graph.
	k := keys.ForGraph(id)
	return s.deleteCellRange(ctx, k.Row, k.Column, 1, math.MaxInt64)
}

// deleteCellRange delete a cell on the big table.
func (s *store) deleteCellRange(ctx context.Context, row, col string, initialTS, finalTS int64) error {
	family, column := parseColumn(col)
	btm := bigtable.NewMutation()
	btm.DeleteTimestampRange(family, column, bigtable.Timestamp(initialTS), bigtable.Timestamp(finalTS))
	if err := s.table.Apply(ctx, row, btm); err != nil {
		return fmt.Errorf(
			"m.t.Apply(_, %s, %s, %d, %d): got (_, %v), wanted (_, nil)",
			row, col, initialTS, finalTS, err)
	}
	return nil
}

// GraphNames returns the current available graph names in the store.
func (s *store) GraphNames(ctx context.Context, names chan<- string) error {
	defer close(names)

	rr := bigtable.PrefixRange(keys.GraphRowPrefix())
	return s.table.ReadRows(ctx, rr, func(row bigtable.Row) bool {
		for _, entries := range row {
			for _, cell := range entries {
				select {
				case <-ctx.Done():
					// We are done.
					return false
				case names <- string(cell.Value):
					// We are not done and properly sent.
				}
			}
		}
		return true
	})
}

// exist returns true if the provided graph ID exists in the table.
func (s *store) exist(ctx context.Context, id string) (bool, error) {
	k := keys.ForGraph(id)
	row, err := s.table.ReadRow(ctx, k.Row)
	if err != nil {
		return false, fmt.Errorf("ReadRow failed: %v", err)
	}
	family, _ := parseColumn(k.Column)
	for _, ri := range row[family] {
		if ri.Column == k.Column {
			return true, nil
		}
	}
	return false, nil
}

// persistBytes stores the provided bytes into the row, column, and cell
// timestamp provided. If the timestamp is 0, the cell will be anchored to
// bigtable.Now().
func (s *store) persistBytes(ctx context.Context, row, col string, tsUsec int64, bs []byte) error {
	btm := bigtable.NewMutation()
	ts := bigtable.Timestamp(tsUsec)
	f, c := parseColumn(col)
	btm.Set(f, c, ts, bs)
	if err := s.table.Apply(ctx, row, btm); err != nil {
		return fmt.Errorf("m.t.Apply(_, %s, %s, %d, _): got (_, %v), wanted (_, nil)", row, col, tsUsec, err)
	}
	return nil
}

// parseColumn separates a column family identifier from a column identifier in
// a colon-delimited string.
// Example:
// "family:col:id" -> ("family", "col:id")
func parseColumn(id string) (string, string) {
	idx := strings.Index(id, ":")
	return id[:idx], id[idx+1:]
}

// graph implements the storage.Graph interface using Bigtable.
type graph struct {
	id                    string
	table                 *bigtable.Table
	bqlChannelSize        int
	bulkTripleBuilderSize int
	bulkTripleOpSize      int
	literalBuilder        literal.Builder
}

// ID returns the id for this graph.
func (g *graph) ID(ctx context.Context) string {
	return g.id
}

// AddTriples adds the triples into storage. Adding triples that already exist
// will not return an error.
func (g *graph) AddTriples(ctx context.Context, ts []*triple.Triple) error {
	var (
		rows []string
		muts []*bigtable.Mutation
	)
	for _, t := range ts {
		ks := keys.ForTriple(g.id, t)
		data := []byte(t.String())
		for _, k := range ks {
			family, column := parseColumn(k.Column)
			btm := bigtable.NewMutation()
			btm.Set(family, column, bigtable.Timestamp(k.Timestamp), data)
			rows = append(rows, k.Row)
			muts = append(muts, btm)
		}
	}

	errs, err := g.table.ApplyBulk(ctx, rows, muts)
	if err != nil {
		return err
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// RemoveTriples removes the specified triples from storage. Removing triples
// that are not present in the store will not return an error.
func (g *graph) RemoveTriples(ctx context.Context, ts []*triple.Triple) error {
	var (
		rows []string
		muts []*bigtable.Mutation
	)
	for _, t := range ts {
		ks := keys.ForTriple(g.id, t)
		for _, k := range ks {
			btm := bigtable.NewMutation()
			btm.DeleteRow()
			rows = append(rows, k.Row)
			muts = append(muts, btm)
		}
	}

	errs, err := g.table.ApplyBulk(ctx, rows, muts)
	if err != nil {
		return err
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// configurableRangeRead provides a generic facility to perform generic range
// reads against the bigtable. Given the row prefix, the column prefix, and
// the lookup options. The process function will get called for each of
// the triples retrieved that require further processing.
func (g *graph) configurableRangeRead(ctx context.Context, rowPrefix, colPrefix string, lo *storage.LookupOptions, process func(*triple.Triple)) error {
	rr := bigtable.PrefixRange(rowPrefix)

	var opts []bigtable.ReadOption

	if lo.MaxElements > 0 {
		// This will likely overestimate the rows returned, but the
		// parser will do another filtering pass later.
		opts = append(opts, bigtable.LimitRows(int64(lo.MaxElements)))
	}

	timeAnchored := false
	lb, ub := int64(0), int64(math.MaxInt64)
	if lo.LowerAnchor != nil {
		lb = lo.LowerAnchor.UnixNano() / 1000
		timeAnchored = true
	}
	if lo.UpperAnchor != nil {
		ub = lo.UpperAnchor.UnixNano() / 1000
		timeAnchored = true
	}
	if timeAnchored {
		opts = append(opts,
			bigtable.RowFilter(
				bigtable.TimestampRangeFilterMicros(bigtable.Timestamp(lb), bigtable.Timestamp(ub))))
	}

	cnt := 0
	processRow := func(row bigtable.Row) bool {
		for _, entries := range row {
			// Use only one of the available indices.
			for _, cell := range entries {
				t, err := triple.Parse(string(cell.Value), literal.DefaultBuilder())
				if err != nil {
					return false
				}
				if lo.MaxElements != 0 && cnt >= lo.MaxElements {
					return false
				}
				process(t)
				cnt++
			}
		}
		return true
	}
	return g.table.ReadRows(ctx, rr, processRow, opts...)
}

// Objects pushes the objects for the given object and predicate to the provided
// channel.
func (g *graph) Objects(ctx context.Context, s *node.Node, p *predicate.Predicate, lo *storage.LookupOptions, objs chan<- *triple.Object) error {
	defer close(objs)

	sUUID, pUUID := s.UUID(), p.PartialUUID()

	// Create the prefix required for the read.
	rowPrefix := keys.PrependPrefix(g.ID(ctx), "SPO", sUUID, pUUID)
	colPrefix := "PO:" + pUUID.String()

	// Start the read.
	return g.configurableRangeRead(ctx, rowPrefix, colPrefix, lo, func(t *triple.Triple) {
		if p.Type() == t.Predicate().Type() {
			select {
			case <-ctx.Done():
				// We are done.
			case objs <- t.Object():
				// We are not done and properly sent.
			}
		}
	})
}

// Subjects pushes the subjects for the given predicate and object to the
// provided channel.
func (g *graph) Subjects(ctx context.Context, p *predicate.Predicate, o *triple.Object, lo *storage.LookupOptions, subs chan<- *node.Node) error {
	defer close(subs)

	pUUID, oUUID := p.PartialUUID(), o.UUID()

	// Create the prefix required for the read.
	rowPrefix := keys.PrependPrefix(g.ID(ctx), "POS", pUUID, oUUID)
	colPrefix := "OS:" + oUUID.String()

	// Start the read.
	visited := make(map[string]bool)
	return g.configurableRangeRead(ctx, rowPrefix, colPrefix, lo, func(t *triple.Triple) {
		if id := t.Subject().UUID().String(); !visited[id] {
			visited[id] = true
			select {
			case <-ctx.Done():
				// We are done.
			case subs <- t.Subject():
				// We are not done and properly sent.
			}
		}
	})
}

// PredicatesForSubject pushes the predicates for the given subject to the
// provided channel.
func (g *graph) PredicatesForSubject(ctx context.Context, s *node.Node, lo *storage.LookupOptions, prds chan<- *predicate.Predicate) error {
	defer close(prds)

	sUUID := s.UUID()

	// Create the prefix required for the read.
	rowPrefix := keys.PrependPrefix(g.ID(ctx), "SPO", sUUID)
	colPrefix := "PO:"

	// Start the read.
	visited := make(map[string]bool)
	return g.configurableRangeRead(ctx, rowPrefix, colPrefix, lo, func(t *triple.Triple) {
		if id := t.Predicate().UUID().String(); !visited[id] {
			visited[id] = true
			select {
			case <-ctx.Done():
				// We are done.
			case prds <- t.Predicate():
				// We are not done and properly sent.
			}
		}
	})
}

// PredicatesForObject pushes the predicates for the given object to the
// provided channel.
func (g *graph) PredicatesForObject(ctx context.Context, o *triple.Object, lo *storage.LookupOptions, prds chan<- *predicate.Predicate) error {
	defer close(prds)

	oUUID := o.UUID()

	// Create the prefix required for the read.
	rowPrefix := keys.PrependPrefix(g.ID(ctx), "OSP", oUUID)
	colPrefix := "SP:"

	// Start the read.
	visited := make(map[string]bool)
	return g.configurableRangeRead(ctx, rowPrefix, colPrefix, lo, func(t *triple.Triple) {
		if id := t.Predicate().UUID().String(); !visited[id] {
			visited[id] = true
			select {
			case <-ctx.Done():
				// We are done.
			case prds <- t.Predicate():
				// We are not done and properly sent.
			}
		}
	})
}

// PredicatesForSubjectAndObject pushes the predicates for the given subject and
// object to the provided channel.
func (g *graph) PredicatesForSubjectAndObject(ctx context.Context, s *node.Node, o *triple.Object, lo *storage.LookupOptions, prds chan<- *predicate.Predicate) error {
	defer close(prds)

	sUUID := s.UUID()

	// Create the prefix required for the read.
	rowPrefix := keys.PrependPrefix(g.ID(ctx), "SOP", sUUID)
	colPrefix := "OP:"

	// Start the read.
	visited := make(map[string]bool)
	return g.configurableRangeRead(ctx, rowPrefix, colPrefix, lo, func(t *triple.Triple) {
		if id := t.Predicate().UUID().String(); !visited[id] {
			visited[id] = true
			select {
			case <-ctx.Done():
				// We are done.
			case prds <- t.Predicate():
				// We are not done and properly sent.
			}
		}
	})
}

// TriplesForSubject pushes the triples with the given subject to the provided
// channel.
func (g *graph) TriplesForSubject(ctx context.Context, s *node.Node, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	defer close(trpls)

	sUUID := s.UUID()

	// Create the prefix required for the read.
	rowPrefix := keys.PrependPrefix(g.ID(ctx), "SPO", sUUID)
	colPrefix := "PO:"

	// Start the read.
	return g.configurableRangeRead(ctx, rowPrefix, colPrefix, lo, func(t *triple.Triple) {
		select {
		case <-ctx.Done():
			// We are done.
		case trpls <- t:
			// We are not done and properly sent.
		}
	})
}

// TriplesForPredicate pushes the triples with the given predicate to the
// provided channel.
func (g *graph) TriplesForPredicate(ctx context.Context, p *predicate.Predicate, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	defer close(trpls)

	pUUID := p.PartialUUID()

	// Create the prefix required for the read.
	rowPrefix := keys.PrependPrefix(g.ID(ctx), "POS", pUUID)
	colPrefix := "OS:"

	// Start the read.
	return g.configurableRangeRead(ctx, rowPrefix, colPrefix, lo, func(t *triple.Triple) {
		if p.Type() == t.Predicate().Type() {
			select {
			case <-ctx.Done():
				// We are done.
			case trpls <- t:
				// We are not done and properly sent.
			}
		}
	})
}

// TriplesForObject pushes the triples with the given object to the provided
// channel.
func (g *graph) TriplesForObject(ctx context.Context, o *triple.Object, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	defer close(trpls)

	oUUID := o.UUID()

	// Create the prefix required for the read.
	rowPrefix := keys.PrependPrefix(g.ID(ctx), "OSP", oUUID)
	colPrefix := "SP:"

	// Start the read.
	return g.configurableRangeRead(ctx, rowPrefix, colPrefix, lo, func(t *triple.Triple) {
		select {
		case <-ctx.Done():
			// We are done.
		case trpls <- t:
			// We are not done and properly sent.
		}
	})
}

// TriplesForSubjectAndPredicate pushes the triples for the given subject and
// predicate to the provided channel.
func (g *graph) TriplesForSubjectAndPredicate(ctx context.Context, s *node.Node, p *predicate.Predicate, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	defer close(trpls)

	sUUID, pUUID := s.UUID(), p.PartialUUID()

	// Create the prefix required for the read.
	rowPrefix := keys.PrependPrefix(g.ID(ctx), "SPO", sUUID, pUUID)
	colPrefix := "PO:" + pUUID.String()

	// Start the read.
	return g.configurableRangeRead(ctx, rowPrefix, colPrefix, lo, func(t *triple.Triple) {
		if p.Type() == t.Predicate().Type() {
			select {
			case <-ctx.Done():
				// We are done.
			case trpls <- t:
				// We are not done and properly sent.
			}
		}
	})
}

// TriplesForPredicateAndObject pushes the triples for the given predicate and
// object to the provided channel.
func (g *graph) TriplesForPredicateAndObject(ctx context.Context, p *predicate.Predicate, o *triple.Object, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	defer close(trpls)

	pUUID, oUUID := p.PartialUUID(), o.UUID()

	// Create the prefix required for the read.
	rowPrefix := keys.PrependPrefix(g.ID(ctx), "POS", pUUID, oUUID)
	colPrefix := "OS:" + oUUID.String()

	// Start the read.
	return g.configurableRangeRead(ctx, rowPrefix, colPrefix, lo, func(t *triple.Triple) {
		if p.Type() == t.Predicate().Type() {
			select {
			case <-ctx.Done():
				// We are done.
			case trpls <- t:
				// We are not done and properly sent.
			}
		}
	})
}

// Exist checks if the provided triple exists in the store.
func (g *graph) Exist(ctx context.Context, t *triple.Triple) (bool, error) {
	s, p, o := t.Subject(), t.Predicate(), t.Object()
	pUUID, oUUID, sUUID := p.PartialUUID(), o.UUID(), s.UUID()

	// Create the prefix required for the read.
	rowPrefix := keys.PrependPrefix(g.ID(ctx), "POS", pUUID, oUUID, sUUID)
	colPrefix := "OS:" + oUUID.String()

	// Start the read.
	found := false
	tUUID := t.UUID()
	err := g.configurableRangeRead(ctx, rowPrefix, colPrefix, storage.DefaultLookup, func(bt *triple.Triple) {
		if uuid.Equal(tUUID, bt.UUID()) {
			found = true
		}
	})
	return found, err
}

// Triples pushes all triples in the store to the provided channel.
func (g *graph) Triples(ctx context.Context, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	defer close(trpls)

	// Create the prefix required for the read.
	rowPrefix := keys.PrependPrefix(g.ID(ctx), "POS")
	colPrefix := "OS:"

	// Start the read.
	return g.configurableRangeRead(ctx, rowPrefix, colPrefix, lo, func(t *triple.Triple) {
		select {
		case <-ctx.Done():
			// We are done.
		case trpls <- t:
			// We are not done and properly sent.
		}
	})
}
