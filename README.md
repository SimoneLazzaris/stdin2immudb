# stdid2immudb 

A simple tool for inserting lines into [immudb](https://github.com/codenotary/immudb) - the immutable database.

For every line fetched on stdin, it will generate Key-Value pair, using as key of the form `LINE000000000`. 
You can change the prefix and the initial counter value using command line options.

Those KVs are accumulated in memory and when the configured batch size is reached, they are written to immudb in a single transaction. You can specify the transaction size using `-batchsize` option.

### Timestamps as keys
As an option, you can have the key composed of the current timestamp, plus a progressive index that is increased on every item in a transaction.

The actual key will be `<prefix><ms_timestamp>.<index>`

Where `<prefix>` is the common prefix (default `LINE`) <ms_timestamp>` is the current epoch in milliseconds and `<index>` is the line position in the transaction that is being build.

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
  -offset int
        Initial counter value
  -pass string
        Password for authenticating to immudb (default "immudb")
  -port int
        Port number of immudb server (default 3322)
  -prefix string
        Prefix for Key generation (default "LINE")
  -readback
        Don't write, read back instead (and check value)
  -timestamp
        Use current epoch as numeric part of the key (instead a progressive integer)
  -user string
        Username for authenticating to immudb (default "immudb")
  -verbose
        Set verbose output

```

## Usage with rsyslog

You can use `omprog` plugin for rsyslog to store log messages in immudb. This is a working configuration that you can use a starting point:
```
module(load="omprog")

*.* {
    action(type="omprog"
        name="rsyslog_auth_immudb"
        binary="/usr/local/bin/stdin2immudb -batchsize 1 -timestamp"
        queue.type="LinkedList"
        queue.size="20000"
        queue.saveOnShutdown="on"
        queue.workerThreads="4"
        queue.workerThreadMinimumMessages="5000"
        action.resumeInterval="5"
        output="/var/log/rsyslog_auth_immudb.log")
}

```

First line is to load omprog module, then there is the `omprog` block. Note that we used `batchsize 1` to avoid waiting for log collection and write immediately log lines to immudb. You can tune that if your log rate is high. 

Note also that we used the `-timestamp` option, so that log lines are indexed by timestamp.

Without that option, on every restart the line count would restart from 0, which is probably not what you want.