package worker

import (
	"context"
	"reflect"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/xieyanran/scannerl-go/internal/rpc/scannerpb"
	"github.com/xieyanran/scannerl-go/internal/target"
)

func TestStreamTargetsDeliversFixture(t *testing.T) {
	want := []target.Target{
		{Host: "10.0.0.1", Port: 80},
		{Host: "example.com", Port: 443, IsDomain: true},
	}
	fake := &fakeScannerServer{}
	for _, w := range want {
		fake.targets = append(fake.targets, scannerpb.ToProtoTarget(w))
	}
	addr := startFakeServer(t, fake)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stream, err := dialFake(t, addr).GetTargets(ctx, &scannerpb.WorkerHello{WorkerId: "w1"})
	if err != nil {
		t.Fatalf("GetTargets: %v", err)
	}

	targets, errs := StreamTargets(ctx, stream)

	var got []target.Target
	for tg := range targets {
		got = append(got, tg)
	}
	for err := range errs {
		t.Fatalf("unexpected error: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestStreamTargetsSurfacesMidStreamError(t *testing.T) {
	fake := &fakeScannerServer{
		targets:    []*scannerpb.Target{scannerpb.ToProtoTarget(target.Target{Host: "10.0.0.1", Port: 80})},
		targetsErr: status.Error(codes.Internal, "boom"),
	}
	addr := startFakeServer(t, fake)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stream, err := dialFake(t, addr).GetTargets(ctx, &scannerpb.WorkerHello{WorkerId: "w1"})
	if err != nil {
		t.Fatalf("GetTargets: %v", err)
	}
	targets, errs := StreamTargets(ctx, stream)

	var got []target.Target
	for tg := range targets {
		got = append(got, tg)
	}
	if len(got) != 1 {
		t.Fatalf("got %d targets before the error, want 1", len(got))
	}

	select {
	case err, ok := <-errs:
		if !ok || err == nil {
			t.Fatal("expected an error on errs, got none")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the mid-stream error")
	}
}
