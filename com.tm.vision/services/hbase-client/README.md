# HBase Client

Go client for HBase CRUD operations using gohbase library.

## Configuration

```go
cfg := &hbaseclient.Config{
    ZookeeperQuorum: "hbase",      // Zookeeper host(s)
    ZookeeperPort:   "2181",       // Zookeeper port
    ZookeeperRoot:   "/hbase",     // Znode parent
}
```

## Usage

### Initialize Client

```go
import hbase "com.tm.vision/com.tm.vision/services/hbase-client"

client, err := hbase.NewClient(cfg)
if err != nil {
    log.Fatal(err)
}
defer client.Close()
```

### Put (Insert/Update)

```go
// Put single cell
err := client.PutCell(ctx, "table", "row1", "cf", "col1", []byte("value"))

// Put multiple cells
values := map[string]map[string][]byte{
    "cf": {
        "col1": []byte("value1"),
        "col2": []byte("value2"),
    },
}
err := client.Put(ctx, "table", "row1", values)
```

### Get

```go
// Get single cell
value, err := client.GetCell(ctx, "table", "row1", "cf", "col1")

// Get entire row
result, err := client.Get(ctx, "table", "row1", nil)

// Get specific columns
families := map[string][]string{
    "cf": {"col1", "col2"},
}
result, err := client.Get(ctx, "table", "row1", families)
```

### Delete

```go
// Delete entire row
err := client.Delete(ctx, "table", "row1", nil)

// Delete single cell
err := client.DeleteCell(ctx, "table", "row1", "cf", "col1")
```

### Scan

```go
// Scan all rows
results, err := client.ScanAll(ctx, "table")

// Scan with range
results, err := client.Scan(ctx, "table", "startRow", "stopRow", nil)

// Process results
for _, r := range results {
    fmt.Println("Row:", r.RowKey)
    for _, cell := range r.Cells {
        fmt.Printf("  %s:%s = %s\n", cell.Family, cell.Qualifier, cell.Value)
    }
}
```

### Check Existence

```go
exists, err := client.Exists(ctx, "table", "row1")
```

## Build

```bash
# Go build
go build ./com.tm.vision/services/hbase-client/...

# Bazel build
bazel build //com.tm.vision/services/hbase-client:hbase-client
```

## Requirements

- HBase cluster with Zookeeper
- Go 1.24+
