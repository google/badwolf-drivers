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
	"testing"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/bttest"
	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/triple/literal"
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

	if got,want:=str.Name(ctx),"bigtable://test:test/badwolf";got!=want{
		t.Errorf("store.Nane(_); got %q, want %q", got, want)
	}
}

func TestStore_Version(t *testing.T) {
	ctx := context.Background()
	str, clean := emptyTestStore(ctx, t)
	defer clean()

	if got,want:=str.Version(ctx),"bwbigtable@"+version;got!=want{
		t.Errorf("store.Version(_); got %q, want %q", got, want)
	}
}