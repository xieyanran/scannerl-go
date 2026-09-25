package worker

import (
	"io"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/xieyanran/scannerl-go/internal/rpc/scannerpb"
)

// fakeScannerServer is a minimal Scanner server test double: GetTargets
// streams a fixed list of Targets (then optionally fails with
// targetsErr); ReportResults records every Result it receives.
type fakeScannerServer struct {
	scannerpb.UnimplementedScannerServer

	targets    []*scannerpb.Target
	targetsErr error

	mu      sync.Mutex
	results []*scannerpb.Result
}

func (f *fakeScannerServer) GetTargets(req *scannerpb.WorkerHello, stream scannerpb.Scanner_GetTargetsServer) error {
	for _, pt := range f.targets {
		if err := stream.Send(pt); err != nil {
			return err
		}
	}
	return f.targetsErr
}

func (f *fakeScannerServer) ReportResults(stream scannerpb.Scanner_ReportResultsServer) error {
	for {
		pr, err := stream.Recv()
		if err == io.EOF {
			f.mu.Lock()
			n := int64(len(f.results))
			f.mu.Unlock()
			return stream.SendAndClose(&scannerpb.ReportAck{Received: n})
		}
		if err != nil {
			return err
		}
		f.mu.Lock()
		f.results = append(f.results, pr)
		f.mu.Unlock()
	}
}

func (f *fakeScannerServer) snapshot() []*scannerpb.Result {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*scannerpb.Result, len(f.results))
	copy(out, f.results)
	return out
}

// startFakeServer serves srv on a real 127.0.0.1:0 listener and returns
// its address. The server is stopped automatically at test cleanup.
func startFakeServer(t *testing.T, srv scannerpb.ScannerServer) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	s := grpc.NewServer()
	scannerpb.RegisterScannerServer(s, srv)
	go s.Serve(ln)
	t.Cleanup(s.Stop)
	return ln.Addr().String()
}

func dialFake(t *testing.T, addr string) scannerpb.ScannerClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return scannerpb.NewScannerClient(conn)
}
