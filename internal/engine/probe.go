package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"syscall"

	"github.com/xieyanran/scannerl-go/internal/fpmodule"
	"github.com/xieyanran/scannerl-go/internal/output"
	"github.com/xieyanran/scannerl-go/internal/target"
)

// probeOne drives one target through Module.Next until it returns a
// Result, replacing the original's statem_tcp/statem_ssl/statem_udp
// gen_statem FSMs with a plain loop. Two source-verified behaviors this
// preserves exactly:
//
//  1. The returned Record always carries t.Host/t.Port (the ORIGINALLY
//     requested target), never the post-Restart ctarget/cport -- matches
//     statem_tcp:terminate/3, which builds the result tuple from
//     Data#args.target/port, not ctarget/cport.
//  2. -r/--retry is a resend-on-empty-round budget that resets on
//     Restart but is NOT replenished between Continue rounds within one
//     connection -- matches statem_tcp:callback/3's retry guard.
//
// Every read/write condition past a successful connect (timeout, EOF,
// socket error) is non-fatal here, matching the original: control always
// returns to Module.Next with whatever was received (possibly nothing),
// and the module decides what that means. The sole exception is a
// Continue step that asked for zero packets but received some anyway,
// which is a hard stop (statem_tcp's toomanypacketreceived) bypassing the
// module, exactly like the source.
func (e *Engine) probeOne(ctx context.Context, t target.Target) output.Record {
	ctarget, cport := t.Host, t.Port
	// state persists across a Restart (carrying forward whatever the
	// module passed to Restart), exactly like the original's moddata --
	// only the very first Next() call ever sees a nil State. Only
	// data/packetRcv/retryBudget reset on each (re)connect.
	var state any

	for { // reconnect loop, re-entered on Restart
		if ctx.Err() != nil {
			return e.record(t, fpmodule.ErrUnknown("cancelled").Result)
		}

		conn, ip, err := e.dialWithRetry(ctx, ctarget, cport)
		if err != nil {
			return e.record(t, classifyConnectErr(err))
		}

		retryBudget := e.cfg.Retry
		var data []byte
		var packetRcv int

		for { // callback loop, driven by Module.Next
			step := e.safeNext(fpmodule.Input{
				Target:    ctarget,
				IP:        ip,
				Port:      cport,
				Args:      e.cfg.ModArgs,
				MaxPkt:    e.cfg.MaxPkt,
				PacketRcv: packetRcv,
				Data:      data,
				State:     state,
				Conn:      conn,
			})

			switch step.Kind {
			case fpmodule.KindResult:
				conn.Close()
				return e.record(t, step.Result)

			case fpmodule.KindRestart:
				conn.Close()
				if step.NewTarget != "" {
					ctarget = step.NewTarget
				}
				if step.NewPort != 0 {
					cport = step.NewPort
				}
				state = step.State
				goto reconnect

			case fpmodule.KindContinue:
				state = step.State
				var hardStop *fpmodule.Result
				data, packetRcv, hardStop = e.sendRecv(conn, step.Payload, step.NumPackets, &retryBudget)
				if hardStop != nil {
					conn.Close()
					return e.record(t, *hardStop)
				}
			}
		}
	reconnect:
	}
}

// safeNext calls cfg.Module.Next, recovering from any panic (nil deref,
// index out of range, a bad module's own bug, ...) and converting it
// into an ErrUnknown Result for this one target instead of letting it
// propagate. Go's default panic semantics crash the entire process, not
// just the offending goroutine, so without this, one bad
// target/module combination would take down every other in-flight and
// already-completed-but-not-yet-flushed result along with it.
func (e *Engine) safeNext(in fpmodule.Input) (step fpmodule.Step) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "engine: recovered panic in %s.Next for %s:%d: %v\n%s",
				e.cfg.ModuleName, in.Target, in.Port, r, debug.Stack())
			step = fpmodule.ErrUnknown(fmt.Sprintf("panic in module: %v", r))
		}
	}()
	return e.cfg.Module.Next(in)
}

func (e *Engine) record(t target.Target, res fpmodule.Result) output.Record {
	return output.Record{Module: e.cfg.ModuleName, Target: t.Host, Port: t.Port, Result: res}
}

// sendRecv writes payload and waits for step.NumPackets reads (or
// fpmodule.Unlimited, meaning "keep reading until one doesn't produce
// anything more"). Once at least one packet has arrived, a further
// timeout/EOF/error simply stops accumulation and returns whatever was
// received so far -- except numPackets == 0 (the module expects nothing
// back), where receiving anything at all is a hard stop.
func (e *Engine) sendRecv(conn *liveConn, payload []byte, numPackets int, retryBudget *int) (data []byte, packetRcv int, hardStop *fpmodule.Result) {
	first := e.sendAndWaitFirst(conn, payload, retryBudget)
	if first == nil {
		return nil, 0, nil
	}
	data = append(data, first...)
	packetRcv = 1

	for numPackets == fpmodule.Unlimited || packetRcv < numPackets {
		chunk, err := conn.readOne(e.cfg.Timeout)
		if err != nil {
			break
		}
		data = append(data, chunk...)
		packetRcv++
	}

	if numPackets == 0 && packetRcv > 0 {
		res := fpmodule.ErrUp([]any{"toomanypacketreceived", string(data)}).Result
		return nil, 0, &res
	}
	return data, packetRcv, nil
}

// sendAndWaitFirst writes payload and waits for the first response chunk,
// resending on an empty round (write failure, or a read that produced
// nothing) while retryBudget allows and payload is non-empty -- matching
// the original, where both a send failure and a zero-packet timeout
// funnel into the same retry-then-fall-back-to-module path. Returns nil
// once retries are exhausted (or unavailable), handing control back to
// the module with nothing received; this is not an error condition.
func (e *Engine) sendAndWaitFirst(conn *liveConn, payload []byte, retryBudget *int) []byte {
	for {
		if err := conn.writeAll(payload, e.cfg.Timeout); err == nil {
			if chunk, err := conn.readOne(e.cfg.Timeout); err == nil {
				return chunk
			}
		}
		if *retryBudget > 0 && len(payload) > 0 {
			*retryBudget--
			continue
		}
		return nil
	}
}

// classifyConnectErr turns a dial failure into a Result, matching
// statem_tcp:connecting/3: econnrefused/econnreset means the target is up
// but refused/reset the connection ({error,up}); anything else (DNS
// failure, other connect errors, context cancellation) means
// reachability is unknown.
func classifyConnectErr(err error) fpmodule.Result {
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) {
		return fpmodule.ErrUp(err.Error()).Result
	}
	return fpmodule.ErrUnknown(err.Error()).Result
}
