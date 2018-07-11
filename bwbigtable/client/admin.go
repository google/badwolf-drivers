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
package client

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/bigtable"
	"github.com/google/badwolf-drivers/bwbigtable/keys"
)

const (
	bwTable = "BadWolf"
)

var (
	columns = []string{
		keys.GraphColumnFamily(),
		keys.ImmutableColumnFamily(),
		keys.TemporalColumnFamily(),
	}
)

// Client implements the client to us to access GCP BigTables.
type Client struct {
	admin *bigtable.AdminClient
	data  *bigtable.Client
	table *bigtable.Table
}

// Must fails if an error is found. Otherwise it returns the provided admin
// client.
func Must(c *Client, err error) *Client {
	if err != nil {
		log.Fatal(err)
	}
	return c
}

// exist returns true if s is in the provide slice.
func existIn(t string, ss []string) bool {
	for _, s := range ss {
		if s == t {
			return true
		}
	}
	return false
}

// TableName returns the master BadWolf table name.
func TableName() string {
	return bwTable
}

// New creates a new admin client for the provide GCP cloud project and
// BigTable instance.
func New(ctx context.Context, project, instance string) (*Client, error) {
	// Make sure we can connect, the table exist, and all column families are
	// present.
	ac, err := bigtable.NewAdminClient(ctx, project, instance)
	if err != nil {
		return nil, err
	}

	tbls, err := ac.Tables(ctx)
	if err != nil {
		return nil, err
	}

	if !existIn(bwTable, tbls) {
		log.Printf("Creating table %q", bwTable)
		if err := ac.CreateTable(ctx, bwTable); err != nil {
			return nil, fmt.Errorf("Could not create table %q; %v", bwTable, err)
		}
	}

	tblInfo, err := ac.TableInfo(ctx, bwTable)
	if err != nil {
		log.Fatalf("Could not read info for table %q: %v", bwTable, err)
	}

	for _, c := range columns {
		if !existIn(c, tblInfo.Families) {
			if err := ac.CreateColumnFamily(ctx, bwTable, c); err != nil {
				return nil, fmt.Errorf(
					"Could not create column family %q in %q; %v", c, bwTable, err)
			}
		}
	}

	// Create the data client and grab the reference to the table.
	dc, err := bigtable.NewClient(ctx, project, instance)
	if err != nil {
		return nil, fmt.Errorf("Could not create data operations client; %v", err)
	}
	tbl := dc.Open(bwTable)

	return &Client{
		admin: ac,
		data:  dc,
		table: tbl,
	}, nil
}

// Table returns the BadWolf master table.
func (c *Client) Table() *bigtable.Table {
	return c.table
}
