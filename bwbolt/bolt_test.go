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

package bwbolt

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"

	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/literal"
	"github.com/google/badwolf/triple/node"
	"github.com/google/badwolf/triple/predicate"
	"go.etcd.io/bbolt"
)

func testDriver(t *testing.T) (storage.Store, *bbolt.DB) {
	path := path.Join(os.TempDir(), fmt.Sprintf("%x-%x.bdb", time.Now().UnixNano(), rand.Int()))
	d, db, err := New(path, literal.DefaultBuilder(), 3*time.Second, false)
	if err != nil {
		t.Fatal(err)
	}
	return d, db
}

func TestGraphManipulation(t *testing.T) {
	ctx := context.Background()
	s, db := testDriver(t)
	defer db.Close()
	// Create new graphs.
	for i := 0; i < 10; i++ {
		if _, err := s.NewGraph(ctx, fmt.Sprintf("test-%d", i)); err != nil {
			t.Errorf("store.NewGraph: should never fail to crate a graph; %s", err)
		}
	}
	// Get an existing graph.
	for i := 0; i < 10; i++ {
		if _, err := s.Graph(ctx, fmt.Sprintf("test-%d", i)); err != nil {
			t.Errorf("store.Graph: should never fail to get an existing graph; %s", err)
		}
	}
	// Delete an existing graph.
	for i := 0; i < 10; i++ {
		if err := s.DeleteGraph(ctx, fmt.Sprintf("test-%d", i)); err != nil {
			t.Errorf("store.DeleteGraph: should never fail to delete an existing graph; %s", err)
		}
	}
	// Get a non existing graph.
	for i := 0; i < 10; i++ {
		if _, err := s.Graph(ctx, fmt.Sprintf("test-%d", i)); err == nil {
			t.Errorf("store.Graph: should never succeed to get a non existing graph; %s", err)
		}
	}
	// Delete an existing graph.
	for i := 0; i < 10; i++ {
		if err := s.DeleteGraph(ctx, fmt.Sprintf("test-%d", i)); err == nil {
			t.Errorf("store.DeleteGraph: should never succed to delete a non existing graph; %s", err)
		}
	}
}

func createTriples(t *testing.T, ss []string) []*triple.Triple {
	ts := []*triple.Triple{}
	for _, s := range ss {
		trpl, err := triple.Parse(s, literal.DefaultBuilder())
		if err != nil {
			t.Fatalf("triple.Parse failed to parse valid triple %s with error %v", s, err)
			continue
		}
		ts = append(ts, trpl)
	}
	return ts
}

func getTestTriples(t *testing.T) []*triple.Triple {
	return createTriples(t, []string{
		"/u<john>\t\"knows\"@[]\t/u<mary>",
		"/u<john>\t\"knows\"@[]\t/u<peter>",
		"/u<john>\t\"knows\"@[]\t/u<alice>",
		"/u<mary>\t\"knows\"@[]\t/u<andrew>",
		"/u<mary>\t\"knows\"@[]\t/u<kim>",
		"/u<mary>\t\"knows\"@[]\t/u<alice>",
	})
}

func TestAddRemoveExistTriples(t *testing.T) {
	ctx, trpls := context.Background(), getTestTriples(t)
	s, db := testDriver(t)
	defer db.Close()

	g, err := s.NewGraph(ctx, "test")
	if err != nil {
		t.Fatalf("store.New failed to create test graph with error %v", err)
	}

	err = g.AddTriples(ctx, trpls)
	if err != nil {
		t.Errorf("graph.AddTriples failed to add triple to the test graph with error %v", err)
	}
	for _, trpl := range trpls {
		exist, err := g.Exist(ctx, trpl)
		if err != nil {
			t.Errorf("graph.Exist failed to find triple %s in test graph with error %v", trpl, err)
		}
		if !exist {
			t.Errorf("graph.Exist failed to find triple %s in test graph", trpl)
		}
	}
	tc := make(chan *triple.Triple)
	go func() {
		err := g.Triples(ctx, storage.DefaultLookup, tc)
		if err != nil {
			t.Fatalf("graph.Triples failed ot retrieve triples from test graph with error %v", err)
		}
	}()
	cnt := 0
	for range tc {
		cnt++
	}
	if got, want := cnt, len(trpls); got != want {
		t.Errorf("graph.Triples failed to retrieve the expected number of triples; got %d, want %d", got, want)
	}

	err = g.RemoveTriples(ctx, trpls)
	if err != nil {
		t.Errorf("graph.RemoveTriples failed to remove triple to the test graph with error %v", err)
	}
	for _, trpl := range trpls {
		exist, err := g.Exist(ctx, trpl)
		if err != nil {
			t.Errorf("graph.Exist failed to not find triple %s in test graph with error %v", trpl, err)
		}
		if exist {
			t.Errorf("graph.Exist found non existent triple %s in test graph", trpl)
		}
	}
	tc = make(chan *triple.Triple)
	go func() {
		err := g.Triples(ctx, storage.DefaultLookup, tc)
		if err != nil {
			t.Fatalf("graph.Triples failed ot retrieve non existent triples from test graph with error %v", err)
		}
	}()
	cnt = 0
	for range tc {
		cnt++
	}
	if got, want := cnt, 0; got != want {
		t.Errorf("graph.Triples failed to retrieve the expected number of triples; got %d, want %d", got, want)
	}
}

var (
	// Anchor bounds.
	minAnchor time.Time
	maxAnchor time.Time
)

func init() {
	minAnchor = time.Unix(-2208988800, 0)  // January 1, 1 at 00:00:00 UTC.
	maxAnchor = time.Unix(253370764799, 0) // December 31, 9999 at 23:59:59 UTC.
}

func TestQueryMethods(t *testing.T) {
	ctx, trpls := context.Background(), getTestTriples(t)
	st, db := testDriver(t)
	defer db.Close()

	g, err := st.NewGraph(ctx, "test")
	if err != nil {
		t.Fatalf("store.New failed to create test graph with error %v", err)
	}

	err = g.AddTriples(ctx, trpls)
	if err != nil {
		t.Errorf("graph.AddTriples failed to add triple to the test graph with error %v", err)
	}

	s, p, o := trpls[0].Subject(), trpls[0].Predicate(), trpls[0].Object()
	los := []*storage.LookupOptions{
		{},
		{
			MaxElements: 1,
		},
		{
			LowerAnchor: &minAnchor,
		},
		{
			LowerAnchor: &minAnchor,
			MaxElements: 1,
		},
		{
			UpperAnchor: &maxAnchor,
		},
		{
			UpperAnchor: &maxAnchor,
			MaxElements: 1,
		},
		{
			LowerAnchor: &minAnchor,
			UpperAnchor: &maxAnchor,
		},
		{
			LowerAnchor: &minAnchor,
			UpperAnchor: &maxAnchor,
			MaxElements: 1,
		},
	}
	cnt := 0
	for _, lo := range los {
		// Check Objects.
		opts := *lo
		objs := make(chan *triple.Object)
		go func() {
			err := g.Objects(ctx, s, p, &opts, objs)
			if err != nil {
				t.Fatalf("graph.Objects(%q, %q) failed with %v", s, p, err)
			}
		}()
		cnt = 0
		for range objs {
			cnt++
		}
		if cnt < 1 {
			t.Errorf("graph.Object(_, %q, %q, _, _) returned no object", s, p)
		}

		// Check Subjects.
		subjs := make(chan *node.Node)
		go func() {
			err := g.Subjects(ctx, p, o, &opts, subjs)
			if err != nil {
				t.Fatalf("graph.Subjects(%q, %q) failed with %v", p, o, err)
			}
		}()
		cnt = 0
		for range subjs {
			cnt++
		}
		if cnt < 1 {
			t.Errorf("graph.Subjects(_, %q, %q, _, _) returned no object", p, o)
		}

		// Check PredicatesForSubject.
		prds := make(chan *predicate.Predicate)
		go func() {
			err := g.PredicatesForSubject(ctx, s, &opts, prds)
			if err != nil {
				t.Fatalf("graph.PredicatesForSubject(_, %q, _, _) failed with %v", s, err)
			}
		}()
		cnt = 0
		for range prds {
			cnt++
		}
		if cnt < 1 {
			t.Errorf("graph.PredicatesForSubject(_, %q, _, _) returned no object", s)
		}

		// Check PredicatesForObject.
		prds = make(chan *predicate.Predicate)
		go func() {
			err := g.PredicatesForObject(ctx, o, &opts, prds)
			if err != nil {
				t.Fatalf("graph.PredicatesForObject(_, %q, _, _) failed with %v", o, err)
			}
		}()
		cnt = 0
		for range prds {
			cnt++
		}
		if cnt < 1 {
			t.Errorf("graph.PredicatesForObject(_, %q, _, _) returned no object", o)
		}

		// Check PredicatesForSubjectAndObject.
		prds = make(chan *predicate.Predicate)
		go func() {
			err := g.PredicatesForSubjectAndObject(ctx, s, o, &opts, prds)
			if err != nil {
				t.Fatalf("graph.PredicatesForSubjecAndObject(_, %q, %q, _, _) failed with %v", s, o, err)
			}
		}()
		cnt = 0
		for range prds {
			cnt++
		}
		if cnt < 1 {
			t.Errorf("graph.PredicatesForSubjecAndObject(_, %q, %q, _, _) returned no object", s, o)
		}

		// Check TriplesForSubject.
		ctrpls := make(chan *triple.Triple)
		go func() {
			err := g.TriplesForSubject(ctx, s, &opts, ctrpls)
			if err != nil {
				t.Fatalf("graph.TriplesForSubject(_, %q, _, _) failed with %v", s, err)
			}
		}()
		cnt = 0
		for range ctrpls {
			cnt++
		}
		if cnt < 1 {
			t.Errorf("graph.TriplesForSubject(_, %q, _, _) returned no object", s)
		}

		// Check TriplesForPredicate.
		ctrpls = make(chan *triple.Triple)
		go func() {
			err := g.TriplesForPredicate(ctx, p, &opts, ctrpls)
			if err != nil {
				t.Fatalf("graph.TriplesForPredicate(_, %q, _, _) failed with %v", p, err)
			}
		}()
		cnt = 0
		for range ctrpls {
			cnt++
		}
		if cnt < 1 {
			t.Errorf("graph.TriplesForPredicate(_, %q, _, _) returned no object", p)
		}

		// Check TriplesForSubjectAndPredicate.
		ctrpls = make(chan *triple.Triple)
		go func() {
			err := g.TriplesForSubjectAndPredicate(ctx, s, p, &opts, ctrpls)
			if err != nil {
				t.Fatalf("graph.TriplesForSubjectAndPredicate(_, %q, %q, _, _) failed with %v", s, p, err)
			}
		}()
		cnt = 0
		for range ctrpls {
			cnt++
		}
		if cnt < 1 {
			t.Errorf("graph.TriplesForSubjectAndPredicate(_, %q, %q, _, _) returned no object", s, p)
		}

		// Check TriplesForPredicateAndObject.
		ctrpls = make(chan *triple.Triple)
		go func() {
			err := g.TriplesForPredicateAndObject(ctx, p, o, &opts, ctrpls)
			if err != nil {
				t.Fatalf("graph.TriplesForPredicateAndObject(_, %q, %q, _, _) failed with %v", p, o, err)
			}
		}()
		cnt = 0
		for range ctrpls {
			cnt++
		}
		if cnt < 1 {
			t.Errorf("graph.TriplesForPredicateAndObject(_, %q, %q, _, _) returned no object", p, o)
		}
	}
}

func TestTriplesForObject(t *testing.T) {
	ctx, trpls := context.Background(), getTestTriples(t)
	st, db := testDriver(t)
	defer db.Close()

	g, err := st.NewGraph(ctx, "test")
	if err != nil {
		t.Fatalf("store.New failed to create test graph with error %v", err)
	}

	err = g.AddTriples(ctx, trpls)
	if err != nil {
		t.Errorf("graph.AddTriples failed to add triple to the test graph with error %v", err)
	}

	opts := &storage.LookupOptions{}
	// Check PredicatesForObject.
	for _, tr := range trpls {
		trps := make(chan *triple.Triple)
		go func() {
			err := g.TriplesForObject(ctx, tr.Object(), opts, trps)
			if err != nil {
				t.Fatalf("graph.PredicatesForObject(_, %q, _, _) failed with %v", tr.Object(), err)
			}
		}()
		cnt := 0
		for range trps {
			cnt++
		}
		if cnt < 1 {
			t.Errorf("graph.TripleForObject(_, %q, _, _) returned no object", tr.Object())
		}

	}
}
