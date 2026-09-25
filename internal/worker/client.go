// Package worker implements the Worker side of scannerl's distributed
// mode: dial a Coordinator, receive a one-time target shard, feed it
// through the unmodified M1 engine, and stream results back.
package worker

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/xieyanran/scannerl-go/internal/output"
	"github.com/xieyanran/scannerl-go/internal/rpc/scannerpb"
	"github.com/xieyanran/scannerl-go/internal/target"
)

// Client bundles one gRPC connection to a Coordinator, reused for both
// the GetTargets and ReportResults RPCs (independent RPCs multiplexed
// over the same HTTP/2 connection).
//
// The connection is deliberately plaintext (insecure.NewCredentials()):
// this milestone does not add authentication or encryption to the
// Coordinator<->Worker channel. See the README's "Known limitation"
// section -- this is an accepted, documented limitation, not an
// oversight.
type Client struct {
	conn *grpc.ClientConn
	rpc  scannerpb.ScannerClient
}

// Dial connects to a Coordinator at addr. The connection is lazy
// (grpc.NewClient does not block until the first RPC), so a dial error
// here only reflects a malformed addr, not reachability.
func Dial(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("worker: dialing coordinator %s: %w", addr, err)
	}
	return &Client{conn: conn, rpc: scannerpb.NewScannerClient(conn)}, nil
}

// Close releases the underlying connection.
func (c *Client) Close() error { return c.conn.Close() }

// GetTargets calls the GetTargets RPC once and adapts its response
// stream into the same (<-chan target.Target, <-chan error) shape
// target.Stream produces.
func (c *Client) GetTargets(ctx context.Context, workerID string) (<-chan target.Target, <-chan error, error) {
	stream, err := c.rpc.GetTargets(ctx, &scannerpb.WorkerHello{WorkerId: workerID})
	if err != nil {
		return nil, nil, fmt.Errorf("worker: calling GetTargets: %w", err)
	}
	targets, errs := StreamTargets(ctx, stream)
	return targets, errs, nil
}

// NewOutput builds an output.Output that streams every Record it's given
// back to the Coordinator over a ReportResults RPC.
func (c *Client) NewOutput(ctx context.Context) output.Output {
	return NewOutput(ctx, c.rpc)
}
