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
  [BoltDB](https://github.com/boltdb/bolt).

## Bulding the command line tool

Assuming you have a fully working installation of GO, you just need to get 
the required packages and build the tool. You can achieve this by typing the 
following commands:

```
$ cd /to/some/new/folder
$ export GOPATH=$PWD
$ go get github.com/boltdb/bolt
$ go get -u cloud.google.com/go/bigtable
$ go get github.com/google/badwolf/...
$ go get github.com/google/btree
$ go get github.com/peterh/liner
$ go get github.com/pborman/uuid
```

These commands should not output anything unless something fails. You can 
check that the new `bw` tools contains the new drivers by checking the 
`--driver` help flag information. You should see drivers other than 
`VOLATILE`

```
 $ ./bin/bw -h
Usage of ./bin/bw:
  -bolt_db_path string
    	The path to the Bolt database to use.
  -bolt_db_read_only
    	Use te Bolt DB only in read only mode.
  -bolt_db_timeout duration
    	The duration of the timeout while opening the Bolt database. (default 3s)
  -bql_channel_size int
    	Internal channel size to use on BQL queries.
  -bt_instance_id string
    	The GCP BigTable instance ID.
  -bt_project_id string
    	The GCP project ID.
  -bt_table_id string
    	The GCP BigTable instance ID. (default "BadWolf")
  -bulk_triple_builder_size_in_bytes int
    	Maximum size of literals when parsing a triple. (default 1000)
  -bulk_triple_op_size int
    	Number of triples to use in bulk load operations. (default 1000)
  -driver string
    	The storage driver to use {VOLATILE|BWBOLT|BWBT}. (default "VOLATILE")
```

For more information about how to use the commands or how the flags work
please see the 
[original documentation](https://github.com/google/badwolf/blob/master/docs/command_line_tool.md). 

## Testing the command line tool

You may always want to run all the test for both repos `badwolf` and 
`badwolf-drivers` to make sure everything is A-Ok. If the tests fail, you 
should consider not using the build tool since it may be tainted.

```
$ go test -race github.com/google/badwolf/... github.com/xllora/bwdrivers/...
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

## GCP BT driver

In order to run this driver you will need to set up a GCP BT instance. You may
want to follow the simple instructions:

1. Set up Cloud Console.
  1. Go to the [Cloud Console](https://cloud.google.com/console) and create or 
     select your project. You will need the project ID later.
  1. Go to **Settings > Project Billing Settings** and enable billing.
  1. Select **APIs & Services > APIs**.
  1. Enable the **Cloud BigTable API** and the **Cloud BigTable Admin API**.
     (You may need to search for the API).
1. Set up gcloud.
  1. `gcloud components update`
  1. `gcloud auth login`
  1. `gcloud config set project PROJECT_ID`
  1. `gcloud auth application-default login`
1. Provision a Cloud BigTable instance
  1. Follow the instructions in the [user 
     documentation](https://cloud.google.com/bigtable/docs/creating-instance) to 
     create a Google Cloud Platform project and Cloud Bigtable instance if 
     necessary.
  1. You'll need to reference your project id and instance id to run the 
     application.
     
Once you have this set up, you should be able to run the command below and 
get dropped on the BQL console.

```
$ bw --driver=BWBT --bt_project_id=PROJECT_ID --bt_instance_id=INSTANCE_ID bql
Welcome to BadWolf vCli (0.9.1-dev)
Using driver "bigtable://bw-dev:bw-dev/BadWolf". Type quit; to exit
Session started at 2018-07-11 13:00:55.172146282 -0700 PDT m=+0.937832480

bql> 

```

### Limitations of the GCP BT driver

> Due to GCP BigTable cell timestamp management there are two issues to keep
> in mind: (1) micros may be rounded up to micros (leading to possible
> collisions), and (2) you may want to avoid using pre epoc time anchors since
> due to cell timestamp management it may also lead to massive collisions. A
> collisions is defined as to triples sharing the same timestamp in the same 
> row. In practice this is not an issue for immutable predicates. For temporal
> ones, you may want to keep in mind the epoch and milis rounding up when 
> planning the time anchors for your triples.
