# stdid2immudb 

A simple tool for inserting lines into immudb.

For every line fetched on stdin, it will generate Key-Value pair, using as key of the form `LINE000000000`.

Those KVs are accumulated in memory and when the configured batch size is reached, they are written to immudb.

## Build

A simple `go build` will do.

## Usage
```
Usage of ./stdin2immudb:
  -addr string
        IP address of immudb server
  -batchsize int
        Batch size (default 1000)
  -db string
        Name of the database to use (default "defaultdb")
  -pass string
        Password for authenticating to immudb (default "immudb")
  -port int
        Port number of immudb server (default 3322)
  -user string
        Username for authenticating to immudb (default "immudb")

```

