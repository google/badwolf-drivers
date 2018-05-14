# BadWolf BigTable driver

This folder contains the driver implementation using 
[Google Cloud BigTable](https://cloud.google.com/bigtable/). If you are
planning to use it directly without BQL, be aware that you will need to drain
the channels passed around the API and/or explicitly closing them. If you do not
do this, _you run the risk of leaking BigTable resources and goroutines in the
process_.

## How does the driver work?

The BigTable is used to index the provied triples to be able to answer all the
queries expressed in the `BadWolf` low level driver defined in
[`storage.graph`](https://github.com/google/badwolf/blob/master/storage/storage.go).

### Schema organization

The basic schema requires you to have three column families on your BigTable.

```
family {
  name: "graph"
  locality_group: "default"
  type: "string"
}
family {
  name: "immutable_triple"
  type: "string"
}
family {
  name: "temporal_triple"
  type: "string"
}
```

*   `graph`: Contains the information about a graph. There is only a cell in the
    `graph:metadata` column. Also this cell is anchored at timestamp 1
    microsecond after the epoch.
*   `immutable_triple`: Contains all the triples whose predicates are immutable.
    The cell contains the triple information and it is set to the timestamp
    described by the hash of the triple's object. This guarantees idempotent
    writes.
*   `temporal_triple`: Contains all the triples whose predicates are temporal.
    The cell contains the triple information and it is set to the timestamp
    described by the time anchor in the predicate. This means, that you have to
    keep in mind that this driver only supports one value for time anchor.

### Row organization

For each triple (`T`) four cells in four different rows will be inserted into
the underlying indexing BigTable. In the rest of this doc we will refer to `S`
as the `subject(T)`, `P` as the `predicate(T)`, and `O` as the `object(T)`. This
four entries actually implement the six required indices to answer all the
drivers queries. To be able to answer all the required queries, you require the
following indices

*   `O`: An index to answer queries that provide the object as a param.
*   `P`: An index to answer queries that provide the predicate as a param.
*   `P, O`: An index to answer queries that provide a predicate and an object as
    params.
*   `S`: An index to answer queries that provide the subject as a param.
*   `S, O`: An index to answer queries that provide a subject and an object as
    params.
*   `S, P`: An index to answer queries that provide a subject and a predicate as
    params.

All this indices can be encoded using four distinct rows and two column prefix.
The four types of rows written to the BigTable take the following form.

*   `<graph_name>:OSP:<O.UUID>:<S.UUID>:<P.PartialUUID>` for index `O`.
*   `<graph_name>:POS:<P.PartialUUID>:<O.UUID>:<S.UUID>` for indices `P` and 
    `P, O`.
*   `<graph_name>:SOP:<S.UUID>:<O.UUID>:<P.PartialUUID>` for indices `S` and 
    `S, O`.
*   `<graph_name>:SPO:<S.UUID>:<P.PartialUUID>:<O.UUID>` for index `S, P`.

This allows to issue read ranges over the first or first and second `UUID` of
the table, and filter against the column prefix for which of the two indices you
need to query in the case of `POS` or `SOP` since they map two indices. You may
wonder why we generate full triple keys for each row instead of using just the
portions required to distinguish the row (e.g. only use `O` `UUID` for `OSP`
index). The answer is simple, that would crate extremely large rows and hot
spots. When scaling the number of triples to billions, index that rely only on
subject, predicate, or object would require call the triples to be stored in
single rows. Those would lead to poor load-balancing behavior, block data
splitting, and degradated replication performance.

Hence, here we traded the efficiency of loading a row in a single shoot (not a
good idea either for memory management when running a server endpoint) for a
little more involved scanning of multiple contiguous rows with proper column
filtering. Hence, these rows represents the six indices required to answer all
possible queries that the `BadWolf` low level driver requires when implementing
[`storage.graph`](https://github.com/google/badwolf/blob/master/storage/storage.go).
