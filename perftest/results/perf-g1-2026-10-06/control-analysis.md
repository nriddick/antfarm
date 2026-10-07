# Identical-control order, drift and contamination analysis

The candidate passes the −3% screen after the prescribed 20-pair follow-ups.
All seven escalated control cells also pass that threshold. However, the
20-pair identical control shows one-sided subthreshold shifts in dtor/0
(−1.40%) and subset1/0 (+0.85%). These are false code-change signals on
byte-identical executables; a passing threshold screen does not establish
an unbiased effect estimate or calibrated confidence. No extra reruns or
runtime changes were made to pursue them.

`control-analysis.json` contains every retained control pair, order-specific
medians, slot ratios, quarter medians, first/last-quarter rate changes,
external CPU statistics, and rejected-reason counts. Pairs are grouped by
mode, percentage, consumer count and attempt number. A whole pair is excluded
if either run is contaminated, exactly as in the gate. Drift compares means
of the first and last quarters; at n=20 each quarter contains five pairs.
The intervals below are the gate's descriptive bootstrap intervals.

| Control cell | Pairs | Median Δ | Interval | Base-first median | Candidate-first median | Second slot faster | Last/first quarter mean Δ |
| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: |
| tree/0, 5-pair | 5 | −2.73% | −7.55…−1.03% | −2.73% | −1.97% | 2/5 | +0.14% |
| tree/0, 20-pair | 20 | +0.97% | −0.10…+2.99% | +0.97% | +0.85% | 12/20 | +0.05% |
| dtor/0, 20-pair | 20 | −1.40% | −1.78…−1.01% | −1.15% | −1.88% | 11/20 | +0.21% |
| subset1/0, 20-pair | 20 | +0.85% | +0.43…+1.35% | +1.00% | +0.69% | 9/20 | −1.05% |

Each 20-pair cell has exactly ten base-first and ten candidate-first pairs.
Dtor's negative shift persists in both orders and each quarter: paired
quarter medians −1.36%, −1.54%, −1.68%, −1.03%. Its base mean changes −0.08%
and candidate mean +0.50% from the first quarter to the last. A simple
first/second-slot advantage or common monotonic warmup trend does not explain
that persistent label-associated deficit. The second-slot median is +0.77%,
but it is faster in only 11/20 pairs and alternating order does not remove
the deficit.

Subset1's paired quarter medians are +0.37%, +0.56%, +0.52%, +1.47%.
Base mean falls 1.48% and candidate mean falls 0.62% from first quarter to
last, so differential drift is visible. The second-slot median is −0.23%,
and the second slot is faster in only 9/20 pairs. Both order-specific
candidate/base medians remain positive. All processes use the same five
warmup cycles and three-second windows; these observations do not identify
warmup as the cause, and no alternative warmup/window experiment was run.

The five-pair tree/0 negative interval does not persist at 20 pairs. The
20-pair tree interval crosses zero, but its raw paired range is −4.20% to
+11.30%, with eight pairs outside ±3%. Its quarter medians are +0.24%,
+3.42%, +1.12%, −0.21%. This is variable process-to-process evidence rather
than a stable universal slot bias.

None of tree/0, dtor/0 or subset1/0's 20-pair attempts was marked
contaminated. Their maximum retained external CPU estimates were 0.107,
0.129 and 0.089 core, respectively, below the 0.25-core rejection threshold.
One dtor pair has +23.57% throughput delta and two subset1 pairs exceed
±3%; these are retained, not screened out by their rate. CPU contamination
screens detect measured interference, not every throughput outlier. The
initial tree/0 −7.55% pair was also retained without a contamination flag.
All raw stdout and CPU/frequency/load observations remain available.

The control executables in every run share SHA256
`9017b3a024392e870920aeda62ad2564fe2b224a4dd6d692d967ab4f61409f57`.
No code difference can explain these control shifts. The data do not isolate
the underlying host/process-position mechanism. Governor control was
unavailable, frequency samples are snapshots rather than effective-frequency
counters, and CPU accounting does not prove complete isolation. The −3%
threshold and exit rules are unchanged. Small one-sided intervals should
not be attributed to an optimization without matched controls.
