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
package bwbigtable

import (
	"context"
	"fmt"
	"testing"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/bttest"
	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/triple"
	"github.com/google/badwolf/triple/literal"
	"github.com/google/badwolf/triple/node"
	"github.com/google/badwolf/triple/predicate"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

const (
	serverAddr = "127.0.0.1:0"
	project    = "proj-test"
	instance   = "test"
	tableName  = "badwolf"
)

func emptyTestStore(ctx context.Context, t *testing.T) (storage.Store, func()) {
	srv, err := bttest.NewServer(serverAddr)
	if err != nil {
		t.Fatalf("bttest.NewServer(%q) failed: %v", serverAddr, err)
	}
	conn, err := grpc.Dial(srv.Addr, grpc.WithInsecure())
	if err != nil {
		srv.Close()
		t.Fatalf("grpc.Dial(_) failed: %v", err)
	}

	admin, err := bigtable.NewAdminClient(ctx, "", "", option.WithGRPCConn(conn))
	if err != nil {
		srv.Close()
		t.Fatalf("bigtable.NewAdminClient(_) failed; %v", err)
	}
	if err := admin.CreateTable(ctx, tableName); err != nil {
		srv.Close()
		t.Fatalf("admin.CreateTable(_) failed; %v", err)
	}
	if err := admin.CreateColumnFamily(ctx, tableName, "immutable_triple"); err != nil {
		srv.Close()
		t.Fatalf("admin.CreateColumnFamily(_) failed; %v", err)
	}
	if err := admin.CreateColumnFamily(ctx, tableName, "temporal_triple"); err != nil {
		srv.Close()
		t.Fatalf("admin.CreateColumnFamily(_) failed; %v", err)
	}
	if err := admin.CreateColumnFamily(ctx, tableName, "graph"); err != nil {
		srv.Close()
		t.Fatalf("admin.CreateColumnFamily(_) failed; %v", err)
	}

	cli, err := bigtable.NewClient(ctx, "", "", option.WithGRPCConn(conn))
	if err != nil {
		srv.Close()
		t.Fatalf("admin.CreateColumnFamily(_) failed; %v", err)
	}
	tbl := cli.Open(tableName)

	f := func() {
		err = admin.DeleteTable(ctx, tableName)
		if err != nil {
			t.Fatal(err)
		}
		srv.Close()
	}
	return &store{
		spec: &TableSpec{
			ProjectID:  project,
			InstanceID: instance,
			TableID:    tableName,
		},
		table:          tbl,
		literalBuilder: literal.DefaultBuilder(),
	}, f
}

func TestFixture(t *testing.T) {
	ctx := context.Background()
	str, clean := emptyTestStore(ctx, t)
	defer clean()

	if str == nil {
		t.Fatal("emptyTestStore should have not return a nil one")
	}
}

func TestStore_Name(t *testing.T) {
	ctx := context.Background()
	str, clean := emptyTestStore(ctx, t)
	defer clean()

	if got, want := str.Name(ctx), "bigtable://proj-test:test/badwolf"; got != want {
		t.Errorf("store.Nane(_); got %q, want %q", got, want)
	}
}

func TestStore_Version(t *testing.T) {
	ctx := context.Background()
	str, clean := emptyTestStore(ctx, t)
	defer clean()

	if got, want := str.Version(ctx), "bwbigtable@"+version; got != want {
		t.Errorf("store.Version(_); got %q, want %q", got, want)
	}
}

func TestGraphNames(t *testing.T) {
	ctx, graphNamePrefix, cnt := context.Background(), "?test_graph", 10
	s, clean := emptyTestStore(ctx, t)
	defer clean()
	for i, max := 0, cnt; i < max; i++ {
		_, err := s.NewGraph(ctx, fmt.Sprintf("%s_%d", graphNamePrefix, i))
		if err != nil {
			t.Error(err)
		}
	}

	names := make(chan string)
	go func() {
		if err := s.GraphNames(ctx, names); err != nil {
			t.Error(err)
		}
	}()

	res := make(map[string]bool)
	for name := range names {
		res[name] = true
	}
	if got, want := len(res), cnt; got != want {
		t.Errorf("store.GraphNames(_) returned the wrong number of names; got %d, want %d; %v", got, want, res)
	}

	for i, max := 0, cnt; i < max; i++ {
		if err := s.DeleteGraph(ctx, fmt.Sprintf("%s_%d", graphNamePrefix, i)); err != nil {
			t.Error(err)
		}
	}
}

func TestNewGraphAndGraphAndDeleteGraph(t *testing.T) {
	ctx, graphName := context.Background(), "?test"
	s, clean := emptyTestStore(ctx, t)
	defer clean()

	g, err := s.NewGraph(ctx, graphName)
	if err != nil {
		t.Error(err)
	}
	if g.ID(ctx) != graphName {
		t.Errorf("store.NewGraph(_, %q) failed to create graph with the right now; got %q, want %q", graphName, g.ID(ctx), graphName)
	}
	got, err := s.(*store).exist(ctx, graphName)
	if err != nil {
		t.Fatal(err)
	}
	if !got {
		t.Errorf("store.exist(_, %q) got %v, want %v", graphName, got, true)
	}
	g2, err := s.NewGraph(ctx, graphName)
	if err == nil {
		t.Errorf("store.NewGraph(_, %q) should have failed to create the graph, returned %v instead", graphName, g2)
	}
	if _, err := s.Graph(ctx, graphName); err != nil {
		t.Errorf("store.Graph(_, %q) should have returned a valid graph, %v", graphName, err)
	}
	if err := s.DeleteGraph(ctx, graphName); err != nil {
		t.Fatalf("store.DeleteGraph(ctx, %q) failed to delete graph, %v", graphName, err)
	}
	if err := s.DeleteGraph(ctx, graphName); err == nil {
		t.Fatalf("store.DeleteGraph(ctx, %q) should have failed to delete a non existent graph", graphName)
	}
}

func getTestTriples(t *testing.T) []*triple.Triple {
	var ts []*triple.Triple

	ss := []string{
		"/some/type<some id>\t\"foo\"@[]\t/some/type<some id>",
		"/some/type<some id>\t\"foo\"@[1985-01-01T00:01:01.999999000Z]\t/some/type<some id>",
		"/some/type<some id>\t\"foo\"@[]\t\"bar\"@[]",
		"/some/type<some id>\t\"foo\"@[]\t\"bar\"@[1985-01-01T00:01:01.999999000Z]",
		"/some/type<some id>\t\"foo\"@[]\t\"true\"^^type:bool",
		"/some/type<some id>\t\"foo\"@[]\t\"1\"^^type:int64",
		"/some/type<some id>\t\"foo\"@[]\t\"1.0\"^^type:float64",
		"/some/type<some id>\t\"foo\"@[]\t\"foo bar\"^^type:text",
		"/some/type<some id>\t\"foo\"@[]\t\"[0 0 0]\"^^type:blob",
		"/some/type<some other id>\t\"foo\"@[]\t/some/type<some id>",
		"/some/type<some other id>\t\"foo\"@[1986-01-01T00:01:01.999999000Z]\t/some/type<some id>",
		"/some/type<some other id>\t\"foo\"@[]\t\"bar\"@[]",
		"/some/type<some other id>\t\"foo\"@[]\t\"bar\"@[1985-01-01T00:01:01.999999000Z]",
		"/some/type<some other id>\t\"foo\"@[]\t\"true\"^^type:bool",
		"/some/type<some other id>\t\"foo\"@[]\t\"1\"^^type:int64",
		"/some/type<some other id>\t\"foo\"@[]\t\"1.0\"^^type:float64",
		"/some/type<some other id>\t\"foo\"@[]\t\"foo bar\"^^type:text",
		"/some/type<some other id>\t\"foo\"@[]\t\"[0 0 0]\"^^type:blob",
	}
	for _, s := range ss {
		tr, err := triple.Parse(s, literal.DefaultBuilder())
		if err != nil {
			t.Fatalf("triple.Parse failed to parse valid test triple %s with error %v", s, err)
		}
		ts = append(ts, tr)
	}

	return ts
}

func TestAddTriplesAndRemoveTriplesAndTriples(t *testing.T) {
	ctx, graphName := context.Background(), "?test"
	s, clean := emptyTestStore(ctx, t)
	defer clean()

	ts := getTestTriples(t)

	// Add test triples and check existence.
	g, err := s.NewGraph(ctx, graphName)
	if err != nil {
		t.Error(err)
	}
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Error(err)
	}

	// Check all triples were added
	tc := make(chan *triple.Triple)
	go func() {
		if err := g.Triples(ctx, storage.DefaultLookup, tc); err != nil {
			t.Error(err)
		}
	}()
	got := 0
	for range tc {
		got++
	}
	if want := len(ts); got != want {
		t.Errorf("g.Triples(_) return the wrong number of triples; got %d, want %d", got, want)
	}

	// Remove triples and check they do not exist any longer.
	if err := g.RemoveTriples(ctx, ts); err != nil {
		t.Error(err)
	}
	tc = make(chan *triple.Triple)
	go func() {
		if err := g.Triples(ctx, storage.DefaultLookup, tc); err != nil {
			t.Error(err)
		}
	}()
	got = 0
	for range tc {
		got++
	}
	if want := 0; got != want {
		t.Errorf("g.Triples(_) return the wrong number of triples; got %d, want %d", got, want)
	}

	// Get rid of the test graph.
	if err = s.DeleteGraph(ctx, graphName); err != nil {
		t.Error(err)
	}
}

func TestAddTriple(t *testing.T) {
	ctx, graphName := context.Background(), "?test"
	s, clean := emptyTestStore(ctx, t)
	defer clean()
	ts := getTestTriples(t)

	// Add test triples and check existence.
	g, err := s.NewGraph(ctx, graphName)
	if err != nil {
		t.Error(err)
	}
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Error(err)
	}

	// Check all triples were added
	tc := make(chan *triple.Triple)
	go func() {
		if err := g.Triples(ctx, storage.DefaultLookup, tc); err != nil {
			t.Error(err)
		}
	}()
	got := 0
	for range tc {
		got++
	}
	if want := len(ts); got != want {
		t.Errorf("g.Triples(_, _) returned the wrong number of triples; got %d, want %d", got, want)
	}

	// Check all the triples were returned correctly.
	gm := make(map[string]bool)
	for _, t := range ts {
		gm[t.UUID().String()] = true
	}

	tc = make(chan *triple.Triple)
	go func() {
		if err := g.Triples(ctx, storage.DefaultLookup, tc); err != nil {
			t.Error(err)
		}
	}()
	diff := make(map[string]bool)
	for trpl := range tc {
		guid := trpl.UUID().String()
		if !gm[guid] {
			t.Errorf("g.Triples(_, _) returned a triple that did not exist on the test set, %s", trpl)
		}
		diff[guid] = true
	}
	if got, want := len(gm), len(diff); got != want {
		t.Errorf("g.Triples(_, _) returned a different set of triples; got %d, want %d", got, want)
	}

	// Get rid of the test graph.
	if err = s.DeleteGraph(ctx, graphName); err != nil {
		t.Error(err)
	}
}

func TestObjects(t *testing.T) {
	ctx, graphName := context.Background(), "?test"
	s, clean := emptyTestStore(ctx, t)
	defer clean()

	ts := getTestTriples(t)

	g, err := s.NewGraph(ctx, graphName)
	if err != nil {
		t.Error(err)
	}
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Error(err)
	}

	// Get all objects.
	n, p, objs := ts[0].Subject(), ts[0].Predicate(), make(chan *triple.Object)
	go func() {
		if err := g.Objects(ctx, n, p, &storage.LookupOptions{}, objs); err != nil {
			t.Error(err)
		}
	}()
	cnt := 0
	for range objs {
		cnt++
	}
	if got, want := cnt, 8; got != want {
		t.Errorf("g.Objects(_,_,_) failed to return the right number of objects; got %v, want %v", got, want)
	}

	// Get only one object.
	n, p, objs = ts[0].Subject(), ts[0].Predicate(), make(chan *triple.Object)
	go func() {
		if err := g.Objects(ctx, n, p, &storage.LookupOptions{MaxElements: 1}, objs); err != nil {
			t.Error(err)
		}
	}()
	cnt = 0
	for range objs {
		cnt++
	}
	if got, want := cnt, 1; got != want {
		t.Errorf("g.Objects(_,_,_) failed to return the right number of objects; got %v, want %v", got, want)
	}

	// Get rid of the test graph.
	if err = s.DeleteGraph(ctx, graphName); err != nil {
		t.Error(err)
	}
}

func TestSubjects(t *testing.T) {
	ctx, graphName := context.Background(), "?test"
	s, clean := emptyTestStore(ctx, t)
	defer clean()

	ts := getTestTriples(t)

	g, err := s.NewGraph(ctx, graphName)
	if err != nil {
		t.Error(err)
	}
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Error(err)
	}

	// Get all subjects.
	p, o, subjs := ts[0].Predicate(), ts[0].Object(), make(chan *node.Node)
	go func() {
		if err := g.Subjects(ctx, p, o, &storage.LookupOptions{}, subjs); err != nil {
			t.Error(err)
		}
	}()
	cnt := 0
	for range subjs {
		cnt++
	}
	if got, want := cnt, 2; got != want {
		t.Errorf("g.Subjects(_,_,_) failed to return the right number of subjects; got %v, want %v", got, want)
	}

	// Get only one object.
	p, o, subjs = ts[0].Predicate(), ts[0].Object(), make(chan *node.Node)
	go func() {
		if err := g.Subjects(ctx, p, o, &storage.LookupOptions{MaxElements: 1}, subjs); err != nil {
			t.Error(err)
		}
	}()
	cnt = 0
	for range subjs {
		cnt++
	}
	if got, want := cnt, 1; got != want {
		t.Errorf("g.Subjects(_,_,_) failed to return the right number of subjects; got %v, want %v", got, want)
	}

	// Get rid of the test graph.
	if err = s.DeleteGraph(ctx, graphName); err != nil {
		t.Error(err)
	}
}

func TestPredicatesForSubjects(t *testing.T) {
	ctx, graphName := context.Background(), "?test"
	s, clean := emptyTestStore(ctx, t)
	defer clean()

	ts := getTestTriples(t)

	g, err := s.NewGraph(ctx, graphName)
	if err != nil {
		t.Error(err)
	}
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Error(err)
	}

	// Get all predicates for subject.
	n, prds := ts[0].Subject(), make(chan *predicate.Predicate)
	go func() {
		if err := g.PredicatesForSubject(ctx, n, &storage.LookupOptions{}, prds); err != nil {
			t.Error(err)
		}
	}()
	cnt := 0
	for range prds {
		cnt++
	}
	if got, want := cnt, 2; got != want {
		t.Errorf("g.PredicatesForSubject(_,_,_) failed to return the right number of subjects; got %v, want %v", got, want)
	}

	// Get only one predicate.
	n, prds = ts[0].Subject(), make(chan *predicate.Predicate)
	go func() {
		if err := g.PredicatesForSubject(ctx, n, &storage.LookupOptions{MaxElements: 1}, prds); err != nil {
			t.Error(err)
		}
	}()
	cnt = 0
	for range prds {
		cnt++
	}
	if got, want := cnt, 1; got != want {
		t.Errorf("g.PredicatesForSubject(_,_,_) failed to return the right number of subjects; got %v, want %v", got, want)
	}

	// Get rid of the test graph.
	if err = s.DeleteGraph(ctx, graphName); err != nil {
		t.Error(err)
	}
}

func TestPredicatesForObject(t *testing.T) {
	ctx, graphName := context.Background(), "?test"
	s, clean := emptyTestStore(ctx, t)
	defer clean()

	ts := getTestTriples(t)

	g, err := s.NewGraph(ctx, graphName)
	if err != nil {
		t.Error(err)
	}
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Error(err)
	}

	// Get all predicates for object.
	o, prds := ts[0].Object(), make(chan *predicate.Predicate)
	go func() {
		if err := g.PredicatesForObject(ctx, o, &storage.LookupOptions{}, prds); err != nil {
			t.Error(err)
		}
	}()
	cnt := 0
	for range prds {
		cnt++
	}
	if got, want := cnt, 3; got != want {
		t.Errorf("g.PredicatesForObject(_,_,_) failed to return the right number of subjects; got %v, want %v", got, want)
	}

	// Get only one predicate.
	o, prds = ts[0].Object(), make(chan *predicate.Predicate)
	go func() {
		if err := g.PredicatesForObject(ctx, o, &storage.LookupOptions{MaxElements: 1}, prds); err != nil {
			t.Error(err)
		}
	}()
	cnt = 0
	for range prds {
		cnt++
	}
	if got, want := cnt, 1; got != want {
		t.Errorf("g.PredicatesForObject(_,_,_) failed to return the right number of subjects; got %v, want %v", got, want)
	}

	// Get rid of the test graph.
	if err = s.DeleteGraph(ctx, graphName); err != nil {
		t.Error(err)
	}
}

func TestPredicatesForSubjectAndObjects(t *testing.T) {
	ctx, graphName := context.Background(), "?test"
	s, clean := emptyTestStore(ctx, t)
	defer clean()

	ts := getTestTriples(t)

	g, err := s.NewGraph(ctx, graphName)
	if err != nil {
		t.Error(err)
	}
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Error(err)
	}

	// Get all predicates for subject and object.
	n, o, prds := ts[0].Subject(), ts[0].Object(), make(chan *predicate.Predicate)
	go func() {
		if err := g.PredicatesForSubjectAndObject(ctx, n, o, &storage.LookupOptions{}, prds); err != nil {
			t.Error(err)
		}
	}()
	cnt := 0
	for range prds {
		cnt++
	}
	if got, want := cnt, 2; got != want {
		t.Errorf("g.PredicatesForSubjectAndObject(_,_,_) failed to return the right number of subjects; got %v, want %v", got, want)
	}

	// Get only one predicate.
	n, o, prds = ts[0].Subject(), ts[0].Object(), make(chan *predicate.Predicate)
	go func() {
		if err := g.PredicatesForSubjectAndObject(ctx, n, o, &storage.LookupOptions{MaxElements: 1}, prds); err != nil {
			t.Error(err)
		}
	}()
	cnt = 0
	for range prds {
		cnt++
	}
	if got, want := cnt, 1; got != want {
		t.Errorf("g.PredicatesForSubjectAndObject(_,_,_) failed to return the right number of subjects; got %v, want %v", got, want)
	}

	// Get rid of the test graph.
	if err = s.DeleteGraph(ctx, graphName); err != nil {
		t.Error(err)
	}
}

func TestTriplesForSubject(t *testing.T) {
	ctx, graphName := context.Background(), "?test"
	s, clean := emptyTestStore(ctx, t)
	defer clean()

	ts := getTestTriples(t)

	g, err := s.NewGraph(ctx, graphName)
	if err != nil {
		t.Error(err)
	}
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Error(err)
	}

	// Get all triples.
	n, tc := ts[0].Subject(), make(chan *triple.Triple)
	go func() {
		if err := g.TriplesForSubject(ctx, n, &storage.LookupOptions{}, tc); err != nil {
			t.Error(err)
		}
	}()
	cnt := 0
	for range tc {
		cnt++
	}
	if got, want := cnt, 9; got != want {
		t.Errorf("g.TriplesForSubject(_,_,_) failed to return the right number of objects; got %v, want %v", got, want)
	}

	// Get only one triple.
	n, tc = ts[0].Subject(), make(chan *triple.Triple)
	go func() {
		if err := g.TriplesForSubject(ctx, n, &storage.LookupOptions{MaxElements: 1}, tc); err != nil {
			t.Error(err)
		}
	}()
	cnt = 0
	for range tc {
		cnt++
	}
	if got, want := cnt, 1; got != want {
		t.Errorf("g.TriplesForSubject(_,_,_) failed to return the right number of objects; got %v, want %v", got, want)
	}

	// Get rid of the test graph.
	if err = s.DeleteGraph(ctx, graphName); err != nil {
		t.Error(err)
	}
}

func TestTriplesForPredicate(t *testing.T) {
	ctx, graphName := context.Background(), "?test"
	s, clean := emptyTestStore(ctx, t)
	defer clean()

	ts := getTestTriples(t)

	g, err := s.NewGraph(ctx, graphName)
	if err != nil {
		t.Error(err)
	}
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Error(err)
	}

	// Get all triples.
	p, tc := ts[0].Predicate(), make(chan *triple.Triple)
	go func() {
		if err := g.TriplesForPredicate(ctx, p, &storage.LookupOptions{}, tc); err != nil {
			t.Error(err)
		}
	}()
	cnt := 0
	for range tc {
		cnt++
	}
	if got, want := cnt, 16; got != want {
		t.Errorf("g.TriplesForPredicate(_,_,_) failed to return the right number of objects; got %v, want %v", got, want)
	}

	// Get only one triple.
	p, tc = ts[0].Predicate(), make(chan *triple.Triple)
	go func() {
		if err := g.TriplesForPredicate(ctx, p, &storage.LookupOptions{MaxElements: 1}, tc); err != nil {
			t.Error(err)
		}
	}()
	cnt = 0
	for range tc {
		cnt++
	}
	if got, want := cnt, 1; got != want {
		t.Errorf("g.TriplesForPredicate(_,_,_) failed to return the right number of objects; got %v, want %v", got, want)
	}

	// Get rid of the test graph.
	if err = s.DeleteGraph(ctx, graphName); err != nil {
		t.Error(err)
	}
}

func TestTriplesForObject(t *testing.T) {
	ctx, graphName := context.Background(), "?test"
	s, clean := emptyTestStore(ctx, t)
	defer clean()

	ts := getTestTriples(t)

	g, err := s.NewGraph(ctx, graphName)
	if err != nil {
		t.Error(err)
	}
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Error(err)
	}

	// Get all triples.
	o, tc := ts[0].Object(), make(chan *triple.Triple)
	go func() {
		if err := g.TriplesForObject(ctx, o, &storage.LookupOptions{}, tc); err != nil {
			t.Error(err)
		}
	}()
	cnt := 0
	for range tc {
		cnt++
	}
	if got, want := cnt, 4; got != want {
		t.Errorf("g.TriplesForObject(_,_,_) failed to return the right number of objects; got %v, want %v", got, want)
	}

	// Get only one triple.
	o, tc = ts[0].Object(), make(chan *triple.Triple)
	go func() {
		if err := g.TriplesForObject(ctx, o, &storage.LookupOptions{MaxElements: 1}, tc); err != nil {
			t.Error(err)
		}
	}()
	cnt = 0
	for range tc {
		cnt++
	}
	if got, want := cnt, 1; got != want {
		t.Errorf("g.TriplesForObject(_,_,_) failed to return the right number of objects; got %v, want %v", got, want)
	}

	// Get rid of the test graph.
	if err = s.DeleteGraph(ctx, graphName); err != nil {
		t.Error(err)
	}
}

func TestTriplesForSubjectAndPredicate(t *testing.T) {
	ctx, graphName := context.Background(), "?test"
	s, clean := emptyTestStore(ctx, t)
	defer clean()

	ts := getTestTriples(t)

	g, err := s.NewGraph(ctx, graphName)
	if err != nil {
		t.Error(err)
	}
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Error(err)
	}

	// Get all triples.
	n, p, tc := ts[0].Subject(), ts[0].Predicate(), make(chan *triple.Triple)
	go func() {
		if err := g.TriplesForSubjectAndPredicate(ctx, n, p, &storage.LookupOptions{}, tc); err != nil {
			t.Error(err)
		}
	}()
	cnt := 0
	for range tc {
		cnt++
	}
	if got, want := cnt, 8; got != want {
		t.Errorf("g.TriplesForSubjectAndPredicate(_,_,_) failed to return the right number of objects; got %v, want %v", got, want)
	}

	// Get only one triple.
	n, p, tc = ts[0].Subject(), ts[0].Predicate(), make(chan *triple.Triple)
	go func() {
		if err := g.TriplesForSubjectAndPredicate(ctx, n, p, &storage.LookupOptions{MaxElements: 1}, tc); err != nil {
			t.Error(err)
		}
	}()
	cnt = 0
	for range tc {
		cnt++
	}
	if got, want := cnt, 1; got != want {
		t.Errorf("g.TriplesForSubjectAndPredicate(_,_,_) failed to return the right number of objects; got %v, want %v", got, want)
	}

	// Get rid of the test graph.
	if err = s.DeleteGraph(ctx, graphName); err != nil {
		t.Error(err)
	}
}

func TestTriplesForPredicateAndObject(t *testing.T) {
	ctx, graphName := context.Background(), "?test"
	s, clean := emptyTestStore(ctx, t)
	defer clean()

	ts := getTestTriples(t)

	g, err := s.NewGraph(ctx, graphName)
	if err != nil {
		t.Error(err)
	}
	if err := g.AddTriples(ctx, ts); err != nil {
		t.Error(err)
	}

	// Get all triples.
	p, o, tc := ts[0].Predicate(), ts[0].Object(), make(chan *triple.Triple)
	go func() {
		if err := g.TriplesForPredicateAndObject(ctx, p, o, &storage.LookupOptions{}, tc); err != nil {
			t.Error(err)
		}
	}()
	cnt := 0
	for range tc {
		cnt++
	}
	if got, want := cnt, 2; got != want {
		t.Errorf("g.TriplesForPredicateAndObject(_,_,_) failed to return the right number of objects; got %v, want %v", got, want)
	}

	// Get only one triple.
	p, o, tc = ts[0].Predicate(), ts[0].Object(), make(chan *triple.Triple)
	go func() {
		if err := g.TriplesForPredicateAndObject(ctx, p, o, &storage.LookupOptions{MaxElements: 1}, tc); err != nil {
			t.Error(err)
		}
	}()
	cnt = 0
	for range tc {
		cnt++
	}
	if got, want := cnt, 1; got != want {
		t.Errorf("g.TriplesForPredicateAndObject(_,_,_) failed to return the right number of objects; got %v, want %v", got, want)
	}

	// Get rid of the test graph.
	if err = s.DeleteGraph(ctx, graphName); err != nil {
		t.Error(err)
	}
}

func TestExist(t *testing.T) {
	ctx, graphName := context.Background(), "?test"
	s, clean := emptyTestStore(ctx, t)
	defer clean()

	ts := getTestTriples(t)

	g, err := s.NewGraph(ctx, graphName)
	if err != nil {
		t.Error(err)
	}
	if err := g.AddTriples(ctx, ts[1:]); err != nil {
		t.Error(err)
	}

	// Check all triples exist.
	for _, trpl := range ts[1:] {
		found, err := g.Exist(ctx, trpl)
		if err != nil {
			t.Error(err)
		}
		if !found {
			t.Errorf("g.Exist(_,_) failed to find triple %v", trpl)
		}
	}

	// Check leave-one-out triple does not exist.
	found, err := g.Exist(ctx, ts[0])
	if err != nil {
		t.Error(err)
	}
	if found {
		t.Errorf("g.Exist(_,_) should have not found triple %v", ts[0])
	}

	// Get rid of the test graph.
	if err = s.DeleteGraph(ctx, graphName); err != nil {
		t.Error(err)
	}
}
