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
package keys

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/literal"
)

const (
	graph = "test"

	textTriples = `/u<joe> "parent_of"@[] /u<mary>
		/u<joe> "parent_of"@[] /u<peter>
		/u<peter> "parent_of"@[] /u<john>
		/u<peter> "parent_of"@[] /u<eve>
		/u<peter> "bought"@[2016-01-01T00:00:00-08:00] /c<mini>
		/u<peter> "bought"@[2016-02-01T00:00:00-08:00] /c<model s>
		/u<peter> "bought"@[2016-03-01T00:00:00-08:00] /c<model x>
		/u<peter> "bought"@[2016-04-01T00:00:00-08:00] /c<model y>
		/c<mini> "is_a"@[] /t<car>
		/c<model s> "is_a"@[] /t<car>
		/c<model x> "is_a"@[] /t<car>
		/c<model y> "is_a"@[] /t<car>
		/l<barcelona> "predicate"@[] "turned"@[2016-01-01T00:00:00-08:00]
		/l<barcelona> "predicate"@[] "turned"@[2016-02-01T00:00:00-08:00]
		/l<barcelona> "predicate"@[] "turned"@[2016-03-01T00:00:00-08:00]
		/l<barcelona> "predicate"@[] "turned"@[2016-04-01T00:00:00-08:00]
		`
)

func mustParse(t *testing.T, raw string) *triple.Triple {
	tpl, err := triple.Parse(raw, literal.DefaultBuilder())
	if err != nil {
		t.Fatal(err)
	}
	return tpl
}

func TestForTriple(t *testing.T) {
	table := []*struct {
		tpl  *triple.Triple
		want []*Indexer
	}{
		{
			tpl: mustParse(t, `/u<joe> "parent_of"@[] /u<mary>`),
			want: []*Indexer{
				{
					Row:       "test:OSP:e5364b94-fe66-5b01-a526-5fdf1e028e80:2821fefc-a86e-597b-b187-58805cad8813:91b6c9d2-69e3-5cec-a465-d200f4872671",
					Column:    "immutable_triple:SP",
					Timestamp: 1000036,
				},
				{
					Row:       "test:POS:91b6c9d2-69e3-5cec-a465-d200f4872671:e5364b94-fe66-5b01-a526-5fdf1e028e80:2821fefc-a86e-597b-b187-58805cad8813",
					Column:    "immutable_triple:OS",
					Timestamp: 1000036,
				},
				{
					Row:       "test:SOP:2821fefc-a86e-597b-b187-58805cad8813:e5364b94-fe66-5b01-a526-5fdf1e028e80:91b6c9d2-69e3-5cec-a465-d200f4872671",
					Column:    "immutable_triple:OP",
					Timestamp: 1000036,
				},
				{
					Row:       "test:SPO:2821fefc-a86e-597b-b187-58805cad8813:91b6c9d2-69e3-5cec-a465-d200f4872671:e5364b94-fe66-5b01-a526-5fdf1e028e80",
					Column:    "immutable_triple:PO",
					Timestamp: 1000036,
				},
			},
		},
		{
			tpl: mustParse(t, `/u<joe> "parent_of"@[] /u<anne>`),
			want: []*Indexer{
				{
					Row:       "test:OSP:adbb5f67-23f0-56a6-82a0-4bc08546ae60:2821fefc-a86e-597b-b187-58805cad8813:91b6c9d2-69e3-5cec-a465-d200f4872671",
					Column:    "immutable_triple:SP",
					Timestamp: 1000006,
				},
				{
					Row:       "test:POS:91b6c9d2-69e3-5cec-a465-d200f4872671:adbb5f67-23f0-56a6-82a0-4bc08546ae60:2821fefc-a86e-597b-b187-58805cad8813",
					Column:    "immutable_triple:OS",
					Timestamp: 1000006,
				},
				{
					Row:       "test:SOP:2821fefc-a86e-597b-b187-58805cad8813:adbb5f67-23f0-56a6-82a0-4bc08546ae60:91b6c9d2-69e3-5cec-a465-d200f4872671",
					Column:    "immutable_triple:OP",
					Timestamp: 1000006,
				},
				{
					Row:       "test:SPO:2821fefc-a86e-597b-b187-58805cad8813:91b6c9d2-69e3-5cec-a465-d200f4872671:adbb5f67-23f0-56a6-82a0-4bc08546ae60",
					Column:    "immutable_triple:PO",
					Timestamp: 1000006,
				},
			},
		},
		{
			tpl: mustParse(t, `/u<joe> "parent_of"@[2017-10-01T12:00:00-08:00] /u<mary>`),
			want: []*Indexer{
				{
					Row:       "test:OSP:e5364b94-fe66-5b01-a526-5fdf1e028e80:2821fefc-a86e-597b-b187-58805cad8813:91b6c9d2-69e3-5cec-a465-d200f4872671",
					Column:    "temporal_triple:SP",
					Timestamp: 1506888000000000000,
				},
				{
					Row:       "test:POS:91b6c9d2-69e3-5cec-a465-d200f4872671:e5364b94-fe66-5b01-a526-5fdf1e028e80:2821fefc-a86e-597b-b187-58805cad8813",
					Column:    "temporal_triple:OS",
					Timestamp: 1506888000000000000,
				},
				{
					Row:       "test:SOP:2821fefc-a86e-597b-b187-58805cad8813:e5364b94-fe66-5b01-a526-5fdf1e028e80:91b6c9d2-69e3-5cec-a465-d200f4872671",
					Column:    "temporal_triple:OP",
					Timestamp: 1506888000000000000,
				},
				{
					Row:       "test:SPO:2821fefc-a86e-597b-b187-58805cad8813:91b6c9d2-69e3-5cec-a465-d200f4872671:e5364b94-fe66-5b01-a526-5fdf1e028e80",
					Column:    "temporal_triple:PO",
					Timestamp: 1506888000000000000,
				},
			},
		},
		{
			tpl: mustParse(t, `/u<joe> "parent_of"@[2017-12-21T12:00:00-08:00] /u<mary>`),
			want: []*Indexer{
				{
					Row:       "test:OSP:e5364b94-fe66-5b01-a526-5fdf1e028e80:2821fefc-a86e-597b-b187-58805cad8813:91b6c9d2-69e3-5cec-a465-d200f4872671",
					Column:    "temporal_triple:SP",
					Timestamp: 1513886400000000000,
				},
				{
					Row:       "test:POS:91b6c9d2-69e3-5cec-a465-d200f4872671:e5364b94-fe66-5b01-a526-5fdf1e028e80:2821fefc-a86e-597b-b187-58805cad8813",
					Column:    "temporal_triple:OS",
					Timestamp: 1513886400000000000,
				},
				{
					Row:       "test:SOP:2821fefc-a86e-597b-b187-58805cad8813:e5364b94-fe66-5b01-a526-5fdf1e028e80:91b6c9d2-69e3-5cec-a465-d200f4872671",
					Column:    "temporal_triple:OP",
					Timestamp: 1513886400000000000,
				},
				{
					Row:       "test:SPO:2821fefc-a86e-597b-b187-58805cad8813:91b6c9d2-69e3-5cec-a465-d200f4872671:e5364b94-fe66-5b01-a526-5fdf1e028e80",
					Column:    "temporal_triple:PO",
					Timestamp: 1513886400000000000,
				},
			},
		},
	}

	for i, entry := range table {
		if got, want := ForTriple(graph, entry.tpl), entry.want; !reflect.DeepEqual(got, want) {
			msg := []string{
				fmt.Sprintf("ForTriple:\nCase %d failed;\n", i),
			}
			for j, max := 0, len(got); j < max; j++ {
				msg = append(msg, fmt.Sprintf("Row:\n\tgot :%v\n\twant:%v\n", got[j].Row, want[j].Row))
				msg = append(msg, fmt.Sprintf("Col:\n\tgot :%v\n\twant:%v\n", got[j].Column, want[j].Column))
				msg = append(msg, fmt.Sprintf("Cts:\n\tgot :%v\n\twant:%v\n", got[j].Timestamp, want[j].Timestamp))
			}
			t.Error(strings.Join(msg, ""))
		}
	}
}

func TestGraphRowPrefix(t *testing.T) {
	if got, want := GraphRowPrefix(), graphRowPrefix; got != want {
		fmt.Errorf("GraphRowPrefix; got %q, want %q", got, want)
	}
}

func TestForGraph(t *testing.T) {
	got := ForGraph(graph)
	want := &Indexer{
		Row:    "graph:" + graph,
		Column: graphMetadataColumn,
	}
	if !reflect.DeepEqual(got, want) {
		fmt.Errorf("ForGraph; got %q, want %q", got, want)
	}
}
