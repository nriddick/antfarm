# Round trip through the ant_uv loop — 30 October 2026

How long a request takes to travel through `ant_uv`: from `UvLoop.submit`
of a `nop` until the waiter runs again. The path covers the request ring,
the loop's wake (`uv_async_send`), one loop iteration, publication, and the
waiter's own wake. A `nop` does no I/O, so this is the loop's overhead on
top of whatever the operation costs.

Ryzen 9 5900XT (two CCDs of eight cores), Linux 7.3, LDC 1.43.0 `-O2
-release`, libuv 1.53.0, Ant Farm `2.2.0-progress`, load average under 0.9.
`tools/perf/uv_latency` with the loop thread pinned to CPU 1, 20,000 timed
requests after 2,000 warm-up requests, each timed with `MonoTime`.

| Waiter | p50 | p90 | p99 | max |
| --- | ---: | ---: | ---: | ---: |
| A thread on CPU 2 spinning on `done` (same CCD) | 0.4–0.5 | 0.5 | 0.5 | 5–7 |
| The same on CPU 9 (other CCD), one run | 1.0 | 1.2 | 4.4 | 17 |
| A worker Fiber parked on a `FiberEvent` (`submitAndWait`) | 6.9–7.0 | 8.0–8.1 | 9.9–10.0 | 20–97 |
| A Fiber on the loop thread (`UvFibers`) | 1.2 | 1.2 | 1.2 | 13–22 |

Microseconds; ranges span three runs. The worker row keeps the loop's CPU
out of the worker set, as the design deploys it. Putting a worker on the
loop's core instead measured 6.5–6.6 µs at p50, so a shared core is not
what the worker row is paying for.

What it shows:

- **The loop costs half a microsecond on one CCD.** That covers the ring,
  the wake, the iteration and publication, observed by a spinning thread.
  Crossing to the other CCD doubles the median and adds a tail.
- **A parked worker Fiber pays about 7 µs.** The loop is a small part of
  it. The rest is waking a parked pool worker (`idleTimeout` parks), the
  lane's flush into its group Farm, the Farm claim and the Fiber switch.
  For file reads, which take tens of microseconds even from the page cache,
  this is overhead well below the operation itself.
- **A Fiber on the loop thread pays 1.2 µs** with a flat tail. It resumes
  on the thread that published, which is the reason to keep a resource
  manager there.

Commands, from `tools/perf` after `make uv_latency`:

```sh
./uv_latency --loop-cpu 1 --cpu 2 --samples 20000   # three times
./uv_latency --loop-cpu 1 --cpu 9 --samples 20000   # the thread row across CCDs
```
