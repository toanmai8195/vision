package hbaseclient

import (
	"context"
	"fmt"
	"io"

	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/hrpc"
)

// Client wraps gohbase client for HBase CRUD operations
type Client struct {
	client gohbase.Client
	config *Config
}

// NewClient creates a new HBase client with the given configuration
func NewClient(cfg *Config) (*Client, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	opts := []gohbase.Option{
		gohbase.ZookeeperRoot(cfg.ZookeeperRoot),
	}

	client := gohbase.NewClient(cfg.GetZookeeperAddress(), opts...)

	return &Client{
		client: client,
		config: cfg,
	}, nil
}

// Close closes the HBase client connection
func (c *Client) Close() {
	c.client.Close()
}

// Put inserts or updates a row in the specified table
func (c *Client) Put(ctx context.Context, table, rowKey string, values map[string]map[string][]byte) error {
	putRequest, err := hrpc.NewPutStr(ctx, table, rowKey, values)
	if err != nil {
		return fmt.Errorf("failed to create put request: %w", err)
	}

	_, err = c.client.Put(putRequest)
	if err != nil {
		return fmt.Errorf("failed to put row: %w", err)
	}

	return nil
}

// Get retrieves a row from the specified table
func (c *Client) Get(ctx context.Context, table, rowKey string, families map[string][]string) (*hrpc.Result, error) {
	var getRequest *hrpc.Get
	var err error

	if families != nil && len(families) > 0 {
		getRequest, err = hrpc.NewGetStr(ctx, table, rowKey, hrpc.Families(families))
	} else {
		getRequest, err = hrpc.NewGetStr(ctx, table, rowKey)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create get request: %w", err)
	}

	result, err := c.client.Get(getRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to get row: %w", err)
	}

	return result, nil
}

// Delete removes a row from the specified table
func (c *Client) Delete(ctx context.Context, table, rowKey string, values map[string]map[string][]byte) error {
	var deleteRequest *hrpc.Mutate
	var err error

	if values != nil && len(values) > 0 {
		deleteRequest, err = hrpc.NewDelStr(ctx, table, rowKey, values)
	} else {
		deleteRequest, err = hrpc.NewDelStr(ctx, table, rowKey, nil)
	}

	if err != nil {
		return fmt.Errorf("failed to create delete request: %w", err)
	}

	_, err = c.client.Delete(deleteRequest)
	if err != nil {
		return fmt.Errorf("failed to delete row: %w", err)
	}

	return nil
}

// ScanResult holds the results of a scan operation
type ScanResult struct {
	RowKey string
	Cells  []*hrpc.Cell
}

// Scan scans rows in the specified table within the given range
func (c *Client) Scan(ctx context.Context, table string, startRow, stopRow string, families map[string][]string) ([]ScanResult, error) {
	var scanRequest *hrpc.Scan
	var err error

	opts := []func(hrpc.Call) error{}

	if families != nil && len(families) > 0 {
		opts = append(opts, hrpc.Families(families))
	}

	if startRow != "" || stopRow != "" {
		scanRequest, err = hrpc.NewScanRangeStr(ctx, table, startRow, stopRow, opts...)
	} else {
		scanRequest, err = hrpc.NewScanStr(ctx, table, opts...)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create scan request: %w", err)
	}

	scanner := c.client.Scan(scanRequest)
	defer scanner.Close()

	var results []ScanResult
	for {
		result, err := scanner.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to scan: %w", err)
		}

		if len(result.Cells) > 0 {
			results = append(results, ScanResult{
				RowKey: string(result.Cells[0].Row),
				Cells:  result.Cells,
			})
		}
	}

	return results, nil
}

// ScanAll scans all rows in the specified table
func (c *Client) ScanAll(ctx context.Context, table string) ([]ScanResult, error) {
	return c.Scan(ctx, table, "", "", nil)
}

// Exists checks if a row exists in the specified table
func (c *Client) Exists(ctx context.Context, table, rowKey string) (bool, error) {
	result, err := c.Get(ctx, table, rowKey, nil)
	if err != nil {
		return false, err
	}

	return len(result.Cells) > 0, nil
}

// PutCell is a convenience method to put a single cell value
func (c *Client) PutCell(ctx context.Context, table, rowKey, family, qualifier string, value []byte) error {
	values := map[string]map[string][]byte{
		family: {
			qualifier: value,
		},
	}
	return c.Put(ctx, table, rowKey, values)
}

// GetCell is a convenience method to get a single cell value
func (c *Client) GetCell(ctx context.Context, table, rowKey, family, qualifier string) ([]byte, error) {
	families := map[string][]string{
		family: {qualifier},
	}

	result, err := c.Get(ctx, table, rowKey, families)
	if err != nil {
		return nil, err
	}

	for _, cell := range result.Cells {
		if string(cell.Family) == family && string(cell.Qualifier) == qualifier {
			return cell.Value, nil
		}
	}

	return nil, nil
}

// DeleteCell is a convenience method to delete a single cell
func (c *Client) DeleteCell(ctx context.Context, table, rowKey, family, qualifier string) error {
	values := map[string]map[string][]byte{
		family: {
			qualifier: nil,
		},
	}
	return c.Delete(ctx, table, rowKey, values)
}
