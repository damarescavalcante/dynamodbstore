# DynamoDB Store 

To test:

```bash
$ go test -v
```

The output like this:

```
=== RUN   TestListBundles
--- PASS: TestListBundles (0.00s)
=== RUN   TestListCAJournals
--- PASS: TestListCAJournals (0.00s)
=== RUN   TestListEntries
--- PASS: TestListEntries (0.00s)
=== RUN   TestListEntryEvents
--- PASS: TestListEntryEvents (0.00s)
=== RUN   TestListFederationRelationships
--- PASS: TestListFederationRelationships (0.00s)
=== RUN   TestListJoinTokens
--- PASS: TestListJoinTokens (0.00s)
=== RUN   TestListNodeEvents
--- PASS: TestListNodeEvents (0.00s)
PASS
ok      dynamodbstore-query-generic     0.005s
```