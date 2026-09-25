package worker

import (
	"context"
	"fmt"

	"github.com/xieyanran/scannerl-go/internal/output"
	"github.com/xieyanran/scannerl-go/internal/rpc/scannerpb"
)

// grpcOutput implements output.Output by forwarding every Write to a
// ReportResults client-streaming RPC. Unlike stdout/json, it is NOT
// registered in internal/output/registry: it isn't something a user
// selects with -o, it's how the worker role wires engine.Run's Sink
// internally (see cmd/scannerl's worker branch), so it's constructed
// directly via NewOutput rather than via output.New.
//
// Like stdoutOutput/jsonOutput, this needs no locking despite being fed
// by many concurrent engine worker goroutines: output.Sink already
// serializes every Write through its own single consumer goroutine, and
// that existing guarantee is exactly what makes an unsynchronized
// stream.Send safe here (grpc's ClientStream.SendMsg is documented as
// not safe for concurrent use).
type grpcOutput struct {
	ctx    context.Context
	client scannerpb.ScannerClient
	stream scannerpb.Scanner_ReportResultsClient
}

// NewOutput builds an output.Output that streams every Record it's given
// to the coordinator reachable through client, over a ReportResults RPC
// opened in Init. ctx is captured so the stream is cancellable by the
// same context the rest of a run is threaded through (Output.Init takes
// no ctx parameter).
func NewOutput(ctx context.Context, client scannerpb.ScannerClient) output.Output {
	return &grpcOutput{ctx: ctx, client: client}
}

func (o *grpcOutput) Init(output.ScanInfo, []string) error {
	stream, err := o.client.ReportResults(o.ctx)
	if err != nil {
		return fmt.Errorf("worker: opening ReportResults stream: %w", err)
	}
	o.stream = stream
	return nil
}

func (o *grpcOutput) Write(rec output.Record) error {
	pr, err := scannerpb.ToProtoResult(rec)
	if err != nil {
		return fmt.Errorf("worker: encoding result: %w", err)
	}
	return o.stream.Send(pr)
}

// Clean closes the send side of the stream and waits for the
// coordinator's ReportAck. output.Sink.Close only calls Clean after
// draining every queued Record (i.e. after every worker goroutine's last
// Write has landed), so this runs exactly once, at exactly the right
// moment.
func (o *grpcOutput) Clean() error {
	_, err := o.stream.CloseAndRecv()
	return err
}

func (o *grpcOutput) Description() string {
	return "internal: forwards results to a coordinator over gRPC"
}

func (o *grpcOutput) Arguments() []string { return nil }
