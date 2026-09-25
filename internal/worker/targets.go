package worker

import (
	"context"
	"io"

	"github.com/xieyanran/scannerl-go/internal/rpc/scannerpb"
	"github.com/xieyanran/scannerl-go/internal/target"
)

// StreamTargets adapts an already-open GetTargets client stream into the
// same (<-chan target.Target, <-chan error) shape target.Stream produces,
// so engine.Run and cmd/scannerl's existing "drain errs concurrently"
// pattern need zero changes to consume it.
func StreamTargets(ctx context.Context, stream scannerpb.Scanner_GetTargetsClient) (<-chan target.Target, <-chan error) {
	out := make(chan target.Target)
	errs := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errs)
		for {
			pt, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				select {
				case errs <- err:
				case <-ctx.Done():
				}
				return
			}
			select {
			case out <- scannerpb.FromProtoTarget(pt):
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, errs
}
