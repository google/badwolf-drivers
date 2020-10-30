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

// Binary btbolt injects the btbolt driver into the bw command line tool.
package main

import (
	"flag"
	"io"
	"log"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/badwolf-drivers/bwbolt"
	"github.com/google/badwolf/storage"
	"github.com/google/badwolf/storage/memory"
	"github.com/google/badwolf/tools/vcli/bw/common"
	"github.com/google/badwolf/tools/vcli/bw/repl"
	"github.com/google/badwolf/triple/literal"
	"github.com/peterh/liner"
	"go.etcd.io/bbolt"
)

var (
	// drivers contains the registered drivers available for this command line tool.
	registeredDrivers map[string]common.StoreGenerator

	// Available flags.
	driverName = flag.String("driver", "VOLATILE", "The storage driver to use {VOLATILE|BWBOLT}.")

	bqlChannelSize        = flag.Int("bql_channel_size", 0, "Internal channel size to use on BQL queries.")
	bulkTripleOpSize      = flag.Int("bulk_triple_op_size", 1000, "Number of triples to use in bulk load operations.")
	bulkTripleBuildersize = flag.Int("bulk_triple_builder_size_in_bytes", 1000, "Maximum size of literals when parsing a triple.")

	// Add your driver flags below.

	// BwBolt driver.
	boltDBPath   = flag.String("bolt_db_path", "", "The path to the Bolt database to use.")
	boldTimeout  = flag.Duration("bolt_db_timeout", 3*time.Second, "The duration of the timeout while opening the Bolt database.")
	boltReadOnly = flag.Bool("bolt_db_read_only", false, "Use te Bolt DB only in read only mode.")

	// Driver specific variables.
	db *bbolt.DB
)

// Registers the available drivers.
func registerDrivers() {
	registeredDrivers = map[string]common.StoreGenerator{
		// Memory only storage driver.
		"VOLATILE": func() (storage.Store, error) {
			return memory.NewStore(), nil
		},
		"BWBOLT": func() (storage.Store, error) {
			s, bdb, err := bwbolt.New(*boltDBPath, literal.DefaultBuilder(), *boldTimeout, *boltReadOnly)
			db = bdb
			return s, err
		},
	}
}

// NewAdvancedReadLiner line editing.
func NewAdvancedReadLiner() repl.ReadLiner {
	return func(done chan bool) <-chan string {
		line, c := liner.NewLiner(), make(chan string)
		usr, err := user.Current()
		if err != nil {
			log.Fatal(err)
		}
		historyFile := filepath.Join(usr.HomeDir, ".badwolf_history")
		go func() {
			defer close(c)
			defer line.Close()

			// Load the history.
			line.SetCtrlCAborts(true)
			line.SetTabCompletionStyle(liner.TabCircular)

			f, err := os.OpenFile(historyFile, os.O_RDONLY, 0600)
			if err != nil {
				f, err = os.OpenFile(historyFile, os.O_CREATE, 0600)
				if err != nil {
					panic("Cannot open history file in " + historyFile)
				}
			}
			line.ReadHistory(f)
			f.Close()

			// Run the read loop.
			cmd := ""
			for {
				text, err := line.Prompt("bql> ")
				if err == liner.ErrPromptAborted || err == io.EOF {
					break
				}
				cmd = strings.TrimSpace(cmd + " " + strings.TrimSpace(text))
				if strings.HasSuffix(cmd, ";") {
					c <- cmd
					line.AppendHistory(cmd)
					if <-done {
						break
					}
					cmd = ""
				}
			}

			// Write the history.
			f, err = os.Create(historyFile)
			if err != nil {
				panic("Cannot rewrite history to " + historyFile + "; " + err.Error())
			}
			defer f.Close()
			line.WriteHistory(f)
		}()
		return c
	}
}

func main() {
	flag.Parse()
	registerDrivers()
	ret := common.Run(*driverName, flag.Args(), registeredDrivers, *bqlChannelSize, *bulkTripleOpSize, *bulkTripleBuildersize, NewAdvancedReadLiner())
	// Clean up.
	if db != nil {
		db.Close()
	}
	os.Exit(ret)
}
