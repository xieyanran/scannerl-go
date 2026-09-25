# scannerl-go

A from-scratch Go reimplementation of [kudelskisecurity/scannerl](https://github.com/kudelskisecurity/scannerl),
a modular fingerprinting engine for probing large numbers of hosts concurrently
(think zmap, but for protocol/service fingerprinting instead of port scanning).

**Status: under active development.** Engine and module set are being ported
from the original Erlang implementation; see the [module list](#modules)
below for current coverage.

This is not a byte-for-byte port: it keeps the original's module contract
(a probe is a pure function from connection state to "send more / reconnect
elsewhere / done") but reimplements everything else idiomatically in Go. See
[Differences from the original](#differences-from-the-original).

## Modules

Fingerprint modules (`-m`), listed via `scannerl -l`:

| Module | Protocol | Default port | Description | Arguments |
| --- | --- | --- | --- | --- |
| `tcpbanner` | TCP | none — specify per target (e.g. `-i host:port`) | grabs the first bytes a TCP service sends unprompted, without sending anything | none |
| `http` | TCP | 80 | sends a GET / request and returns the HTTP status line and headers | none |
| `https_certif` | SSL/TLS | 443 | performs a TLS handshake (certificate validation disabled) and returns the peer certificate's subject, issuer, validity window and DNS SANs | none |

Outputs (`-o`), listed via `scannerl -l`:

| Output | Description | Arguments |
| --- | --- | --- |
| `stdout` | prints one line per result to stdout | none |
| `json` | prints one JSON object per result to stdout (JSON-lines) | none |

## Distributed mode

A single Coordinator can push a static, one-time shard of targets out to N
Workers, which fingerprint their shard locally and stream results back over
gRPC; the Coordinator merges every Worker's results into one unified output
stream, exactly like a single-host scan. There is no dynamic rebalancing
after the initial push — if a worker finishes early, it sits idle (mirroring
the original scannerl's `master.erl` `push/3`).

Coordinator:

    scannerl -role coordinator -workers 3 -listen :9090 -m https_certif -o json -i 10.0.0.0/24

Worker (run this on each of the 3 machines, pointed at the coordinator):

    scannerl -role worker -connect coordinator-host:9090 -m https_certif -P 512

**`-m` must match between the coordinator and every worker** — the
coordinator only uses it to pick the target port default; each worker
independently uses it to run that module. A mismatch isn't rejected
automatically in this release. `-o`/`-i`/`-f` are ignored in worker role
(results always stream to the coordinator; targets always come from it).

### Known limitation: no authentication or encryption

The Coordinator↔Worker gRPC channel is deliberately unauthenticated and
unencrypted (`insecure.NewCredentials()` on both sides) in this release.
Anyone who can reach the Coordinator's `-listen` address can pose as a
Worker (learning the scan's target list) or pose as a Worker feeding
fabricated results into the Coordinator's output. **Only run distributed
mode on a trusted network** (e.g. inside a private VPC/VPN with no public
exposure of the listen port) until TLS + authentication are added. This is
an accepted, tracked limitation, not an oversight — see
[#13](https://github.com/xieyanran/scannerl-go/issues/13) for the
follow-up (verifying this concern's real-world impact on actual
multi-host infrastructure is tracked separately from the milestone that
introduced distributed mode).

## Differences from the original

- **Distributed mode** exists (see above) but, unlike the original's
  SSH-based `-s`/`--slave` node list, Workers dial in to a Coordinator over
  gRPC rather than the Coordinator connecting out to a known host list.
- **License**: MIT here, vs. the original's GPLv3 (this is an independent
  reimplementation; no original source was copied).

## License

MIT — see [LICENSE](LICENSE).
