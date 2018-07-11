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

// Package keys implements the mechanics that maps a triple to the collection
// of keys to be used to store triples on Google Cloud BigTable. It also
// provides the serialized version of the triple that will be stored on the
// table.
package keys

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"strings"
	"time"

	"fmt"
	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/predicate"
	"github.com/pborman/uuid"
)

const (
	graphColumnFamily           = "graph"
	graphRowPrefix              = graphColumnFamily + ":"
	graphMetadataColumn         = "graph:metadata"
	immutableTripleColumnFamily = "immutable_triple"
	temporalTripleColumnFamily  = "temporal_triple"
)

var (
	graphCellTimestamp        = time.Unix(0, 1714)
	graphCellTimestampInInt64 = graphCellTimestamp.UnixNano()
)

// Indexer contains the mapping to a given row+column combination.
type Indexer struct {
	// Row key in BigTable.
	Row string
	// Column name in BigTable.
	Column string
	// Timestamp to use for the cell
	Timestamp int64
}

// String returns a human readable version of the data contained on the
// Indexer instance.
func (i *Indexer) String() string {
	return "<r/" + i.Row + " c/" + i.Column + " ta/" + fmt.Sprint(i.Timestamp) + ">"
}

// PrependPrefix composes returns a composed string base on the parts provided.
func PrependPrefix(graph string, prefix string, UUIDs ...uuid.UUID) string {
	gb := bytes.NewBufferString(graph)
	gb.WriteString(":")

	gb.WriteString(prefix)
	if len(UUIDs) > 0 {
		gb.WriteString(":")
	}

	var sUUIDs []string
	for _, id := range UUIDs {
		sUUIDs = append(sUUIDs, id.String())
	}
	gb.WriteString(strings.Join(sUUIDs, ":"))

	return gb.String()
}

// tripleColumnFamily returns the right column family to use.
func tripleColumnFamily(p *predicate.Predicate) string {
	if p.Type() == predicate.Immutable {
		return immutableTripleColumnFamily
	}
	return temporalTripleColumnFamily
}

// cellIndexerKeys returns an indexer entry based on the provided information.
func cellIndexerKeys(g string, rowPrefix, columnFamily, columnPrefix string, ts int64, rowUUIDs, _ []uuid.UUID) *Indexer {
	return &Indexer{
		Row:       PrependPrefix(g, rowPrefix, rowUUIDs...),
		Column:    PrependPrefix(columnFamily, columnPrefix),
		Timestamp: ts,
	}
}

// ForTriple returns all the associate indexing keys for the given triple
// in a given graph.
func ForTriple(g string, t *triple.Triple) []*Indexer {
	s, p, o := t.Subject().UUID(), t.Predicate().PartialUUID(), t.Object().UUID()
	tcf := tripleColumnFamily(t.Predicate())
	cta := CellTimestamp(t)

	return []*Indexer{
		cellIndexerKeys(
			g, "OSP", tcf, "SP", cta,
			[]uuid.UUID{o, s, p},
			[]uuid.UUID{s, p}),
		cellIndexerKeys(
			g, "POS", tcf, "OS", cta,
			[]uuid.UUID{p, o, s},
			[]uuid.UUID{o, s}),
		cellIndexerKeys(
			g, "SOP", tcf, "OP", cta,
			[]uuid.UUID{s, o, p},
			[]uuid.UUID{o, p}),
		cellIndexerKeys(
			g, "SPO", tcf, "PO", cta,
			[]uuid.UUID{s, p, o},
			[]uuid.UUID{p, o}),
	}
}

// ForGraph returns the key for the graph entity.
func ForGraph(g string) *Indexer {
	return &Indexer{
		Row:       "graph:" + g,
		Column:    graphMetadataColumn,
		Timestamp: graphCellTimestampInInt64, // Stable anchor.
	}
}

// GraphRowPrefix returns the column family prefix including the ':'
func GraphRowPrefix() string {
	return graphRowPrefix
}

// CellTimestamp returns the timestamp to use for the cell.
func CellTimestamp(t *triple.Triple) int64 {
	if ta, err := t.Predicate().TimeAnchor(); err == nil && ta != nil {
		return ta.UnixNano()
	}
	h := sha256.Sum256([]byte(t.String()))
	nsec, _ := binary.Varint(h[:])
	return nsec
}

// GraphColumnFamily returns the graph column family.
func GraphColumnFamily() string {
	return graphColumnFamily
}

// ImmutableColumnFamily returns the immutable predicate column family.
func ImmutableColumnFamily() string {
	return immutableTripleColumnFamily
}

// TemporalColumnFamily returns the temporal predicate column family.
func TemporalColumnFamily() string {
	return temporalTripleColumnFamily
}
