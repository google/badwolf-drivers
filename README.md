# BadWolf Drivers

[![Build Status](https://travis-ci.org/google/badwolf-drivers.svg?branch=master)](https://travis-ci.org/google/badwolf-drivers) [![Go Report Card](https://goreportcard.com/badge/github.com/google/badwolf-drivers)](https://goreportcard.com/report/github.com/google/badwolf-drivers) [![GoDoc](https://godoc.org/github.com/google/badwolf-drivers?status.svg)](https://godoc.org/github.com/google/badwolf-drivers)


BadWolf is a temporal graph store loosely modeled after the concepts introduced
by the
[Resource Description Framework (RDF)](https://en.wikipedia.org/wiki/Resource_Description_Framework).
It presents a flexible storage abstraction, efficient query language, and
data-interchange model for representing a directed graph that accommodates the
storage and linking of arbitrary objects without the need for a rigid schema.

[BadWolf](https://github.com/google/badwolf) main repository contains the
implementation, but no persistent drivers. This repository contains a collection
of driver implementations aim to provide, mainly, persistent storage
alternatives.

## Currently available driver implementations

* `bwbolt` stores and indexes all triples using the key value store
  [BoltDB](https://github.com/etcd-io/bbolt).

## Building the command line tool

Assumming you have a fully working installation of GO that supports golang modules>

```
$ cd /to/some/new/folder
$ go install github.com/google/badwolf-drivers/...
```

These commands should not output anything unless something fails. You can
check that the new `bw` tools contains the new drivers by checking the
`--driver` help flag information. You should see drivers other than
`VOLATILE`

```
 $ $GOPATH/bin/bw -h
Usage of /usr/local/go/bin/bw:
  -bolt_db_path string
    	The path to the Bolt database to use.
  -bql_channel_size int
    	Internal channel size to use on BQL queries.
  -driver string
    	The storage driver to use {VOLATILE|BWBOLT}. (default "VOLATILE")
```

For more information about how to use the commands or how the flags work
please see the
[original documentation](https://github.com/google/badwolf/blob/master/docs/command_line_tool.md).

## Testing the command line tool

You may always want to run all the test for both repos `badwolf` and
`badwolf-drivers` to make sure eveything is A-Ok. If the tests fail, you
should consider not using the build tool since it may be tainted.

```
$ go test -race github.com/google/badwolf/... github.com/google/badwolf-drivers/...
ok  	github.com/google/badwolf/bql/grammar	1.175s
ok  	github.com/google/badwolf/bql/lexer	1.045s
ok  	github.com/google/badwolf/bql/planner	1.531s
ok  	github.com/google/badwolf/bql/semantic	1.081s
ok  	github.com/google/badwolf/bql/table	1.076s
?   	github.com/google/badwolf/bql/version	[no test files]
ok  	github.com/google/badwolf/io	1.054s
?   	github.com/google/badwolf/storage	[no test files]
ok  	github.com/google/badwolf/storage/memory	1.071s
ok  	github.com/google/badwolf/tools/compliance	1.170s
?   	github.com/google/badwolf/tools/vcli/bw	[no test files]
?   	github.com/google/badwolf/tools/vcli/bw/assert	[no test files]
?   	github.com/google/badwolf/tools/vcli/bw/command	[no test files]
?   	github.com/google/badwolf/tools/vcli/bw/common	[no test files]
?   	github.com/google/badwolf/tools/vcli/bw/io	[no test files]
?   	github.com/google/badwolf/tools/vcli/bw/run	[no test files]
?   	github.com/google/badwolf/tools/vcli/bw/version	[no test files]
ok  	github.com/google/badwolf/triple	1.053s
ok  	github.com/google/badwolf/triple/literal	1.044s
ok  	github.com/google/badwolf/triple/node	1.047s
ok  	github.com/google/badwolf/triple/predicate	1.020s
?   	github.com/google/badwolf-drivers/bw	[no test files]
ok  	github.com/google/badwolf-drivers/bwbolt	1.277s
```
