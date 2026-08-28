Captured Windows topology blobs from SISYPHUS (i7-12700H).

Host
  ProductName     Windows 10 Home
  DisplayVersion  25H2
  ReleaseId       2009 (frozen)
  CurrentBuild    26200
  UBR             9168
  CPU             12th Gen Intel Core i7-12700H
                  Family 6 Model 154 Stepping 3
                  CPUID BFEBFBFF000906A3

sisyphus_cpusets.bin
  Raw buffer from GetSystemCpuSetInformation (20 records, 32 bytes each,
  640 bytes). Do not call the live API in topology_parse unittests; parse
  this file instead.

sisyphus_slpiex.bin
  Raw buffer from GetLogicalProcessorInformationEx(RelationAll), 3304 bytes.
  Walk records by the Size field; do not increment a typed pointer.

Expected parse
  1 process-wide LLC (24 MiB, line size 64)
  20 logical processors
  6 SMT P-cores (LPs 0-11, EfficiencyClass 1)
  8 non-SMT E-cores (LPs 12-19, EfficiencyClass 0)
  two L2 modules: LPs 12-15 and 16-19
  llcIndex == 0 and llcIndexInGroup == 0 for every LP

Parked (AllFlags bit 0) is a momentary scheduler bit, not SMT.
