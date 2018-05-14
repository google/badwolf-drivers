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
	"errors"
	"fmt"
	"strings"

	"cloud.google.com/go/bigtable"
	"github.com/google/badwolf-drivers/bwbigtable/keys"
	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/literal"
	"github.com/google/badwolf/triple/node"
	"github.com/google/badwolf/triple/predicate"
)

const (
	version                = "0.1.0-dev"
	graphMetadataTimestamp = 1
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
		s.spec.InstanceID, s.spec.InstanceID, s.spec.TableID)
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
	if err := s.persistBytes(ctx, k.Row, k.Column, graphMetadataTimestamp, []byte(id)); err != nil {
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
	return errors.New("not implemented")
}

// GraphNames returns the current available graph names in the store.
func (s *store) GraphNames(ctx context.Context, names chan<- string) error {
	defer close(names)

	rr := bigtable.PrefixRange(keys.GraphRowPrefix() + ":")
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
	for _, ri := range row[keys.GraphRowPrefix()] {
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
	return id[:idx], id[idx:]
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
	return errors.New("not implemented")
}

// RemoveTriples removes the specified triples from storage. Removing triples
// that are not present in the store will not return an error.
func (g *graph) RemoveTriples(ctx context.Context, ts []*triple.Triple) error {
	return errors.New("not implemented")
}

// Objects pushes the objects for the given object and predicate to the provided
// channel.
func (g *graph) Objects(ctx context.Context, s *node.Node, p *predicate.Predicate, lo *storage.LookupOptions, objs chan<- *triple.Object) error {
	return errors.New("not implemented")
}

// Subjects pushes the subjects for the given predicate and object to the
// provided channel.
func (g *graph) Subjects(ctx context.Context, p *predicate.Predicate, o *triple.Object, lo *storage.LookupOptions, subs chan<- *node.Node) error {
	return errors.New("not implemented")
}

// PredicatesForSubject pushes the predicates for the given subject to the
// provided channel.
func (g *graph) PredicatesForSubject(ctx context.Context, s *node.Node, lo *storage.LookupOptions, prds chan<- *predicate.Predicate) error {
	return errors.New("not implemented")
}

// PredicatesForObject pushes the predicates for the given object to the
// provided channel.
func (g *graph) PredicatesForObject(ctx context.Context, o *triple.Object, lo *storage.LookupOptions, prds chan<- *predicate.Predicate) error {
	return errors.New("not implemented")
}

// PredicatesForSubjectAndObject pushes the predicates for the given subject and
// object to the provided channel.
func (g *graph) PredicatesForSubjectAndObject(ctx context.Context, s *node.Node, o *triple.Object, lo *storage.LookupOptions, prds chan<- *predicate.Predicate) error {
	return errors.New("not implemented")
}

// TriplesForSubject pushes the triples with the given subject to the provided
// channel.
func (g *graph) TriplesForSubject(ctx context.Context, s *node.Node, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	return errors.New("not implemented")
}

// TriplesForPredicate pushes the triples with the given predicate to the
// provided channel.
func (g *graph) TriplesForPredicate(ctx context.Context, p *predicate.Predicate, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	return errors.New("not implemented")
}

// TriplesForObject pushes the triples with the given object to the provided
// channel.
func (g *graph) TriplesForObject(ctx context.Context, o *triple.Object, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	return errors.New("not implemented")
}

// TriplesForSubjectAndPredicate pushes the triples for the given subject and
// predicate to the provided channel.
func (g *graph) TriplesForSubjectAndPredicate(ctx context.Context, s *node.Node, p *predicate.Predicate, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	return errors.New("not implemented")
}

// TriplesForPredicateAndObject pushes the triples for the given predicate and
// object to the provided channel.
func (g *graph) TriplesForPredicateAndObject(ctx context.Context, p *predicate.Predicate, o *triple.Object, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	return errors.New("not implemented")
}

// Exist checks if the provided triple exists in the store.
func (g *graph) Exist(ctx context.Context, t *triple.Triple) (bool, error) {
	return false, errors.New("not implemented")
}

// Triples pushes all triples in the store to the provided channel.
func (g *graph) Triples(ctx context.Context, lo *storage.LookupOptions, trpls chan<- *triple.Triple) error {
	return errors.New("not implemented")
}
