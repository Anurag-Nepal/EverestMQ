# EverestMQ High-Performance Benchmark Report

## Test Configuration
- **Messages:** 10000
- **Payload Size:** 100 bytes
- **Batch Size:** 200
- **OS:** Windows 11
- **Java:** 21.0.2

## Results

| Policy | Producer (msg/sec) | Consumer (msg/sec) | Avg Latency (ms) | Min/Max (ms) |
|--------|-------------------|-------------------|------------------|--------------|
| NONE | 3,543 | 672 | 3799.86 | 15 / 12046 |
| RECEIVED | 4,012 | 672 | 3622.49 | 2 / 11921 |
| PERSISTED | 77,450 | 382 | 135.52 | 0 / 821 |

## Data Integrity
- **NONE:** Received 10000/10000
- **RECEIVED:** Received 10000/10000
- **PERSISTED:** Received 7697/10000
