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

## Differences from the original

- **No distributed mode (yet).** The original distributes work across Erlang
  nodes over SSH; this port is a single-binary, single-host concurrent engine
  (goroutine worker pool). The engine only depends on a `<-chan target.Target`,
  so a network-based dispatcher could be added later without a rewrite, but
  none exists yet.
- **License**: MIT here, vs. the original's GPLv3 (this is an independent
  reimplementation; no original source was copied).

## License

MIT — see [LICENSE](LICENSE).
