# Spark audiences

Spark job (Scala API) that builds advertiser audiences from raw DSP logs. It writes one Parquet dataset per audience with `user_id`, `user_id_type`, `audience_id`, `generated_at`.

Scala 2.12, Spark 3.5 (provided by the cluster), Java 17.

## Run

```bash
sbt test
sbt package
spark-submit --class audiences.AudienceJob target/scala-2.12/spark-audiences_2.12-0.1.0.jar \
  --input s3://bucket/rawlog/ --output s3://bucket/audiences/ --run-date 2026-07-11
```

Local run without a cluster: `sbt "run --input data --output out --run-date 2026-07-11"`.

| Option | Default | Meaning |
|---|---|---|
| `--run-date` | required | last day of the 7-day window |
| `--time-zone` | `UTC` | zone for days and hours |
| `--output-files` | `1` | Parquet files per audience |
| `--impressions-from`, `--impressions-to` | `2026-07-05`, `2026-07-11` | window of audience A |

Output: `<output>/audience_A`, `audience_B`, `audience_C`. A rerun overwrites them, so the job is idempotent.

## How it works

```
raw logs ──scan + one shuffle──> user-day table ──small rollups──> A, B, C
```

- **One scan, one shuffle.** `UserDaily` groups events by user and day into counters: impressions, clicks, bid requests, evening bid requests from US/CA, and up to 3 distinct sites. The table is persisted, and each audience is a small rollup of it.
- **Read only what is needed.** The scan reads 15 of 122 columns. The time window is a plain range on the raw `time` column, so it is pushed down to Parquet row-group statistics (`PushedFilters: [GreaterThanOrEqual(time, ...), LessThan(time, ...)]` in the plan).
- **Bounded site sets.** Per-day site sets are capped at 3 values. The union of capped sets reaches 3 exactly when the full union does, so one noisy user cannot blow up memory.
- **Output files.** `repartition(n)` keeps the aggregation parallel. `coalesce(1)` would pull the whole upstream stage into one task.

## Identity resolution

| Traffic | `user_id_type` | `user_id` |
|---|---|---|
| in-app with an advertising ID | `device_id` | `md5(lower(ifa))` |
| web, or in-app without a usable IFA (empty or zeroed by limit ad tracking) | `ip_ua` | `md5(ip + "\|" + user_agent)` |

IP is taken from `userIp`, then `ip`, then `ipv6`; User-Agent from `userUa`, then `userAgent`. Rows with neither key are skipped. An IP never contains `|`, so the separator cannot cause collisions.

## Assumptions

- Each row is one event marked by counter columns. The job sums `impr`, `click` and `bidRequest`, so pre-aggregated rows also work.
- **A:** impressions are `impr`. The window is 5-11 Jul 2026 inclusive; the year is taken from the data. "Never clicked" means no clicks in the same window. A longer click history is a lookback over the stored user-day table (see below).
- **B:** "more than 50" means `> 50`. A site is the site ID, else the app bundle, else the domain; the site ID is empty in the sample.
- **C:** "US & Canada" means `countryIso3` is `USA` or `CAN` for each counted request. 18 to 23 means 18:00-22:59, and the condition must hold on each of the 7 days.
- "Last 7 days" means the run date and the 6 days before it, as calendar days in `--time-zone`. Each user's local time would need a geo-to-time-zone lookup; region is empty in the sample.
- Events are not deduplicated. If logging is at-least-once, deduplicate by `(requestId, impId, event type)` before the aggregation.

## Sample data

The sample has 957 rows covering 4 minutes on 11 Jul 2026 (20:23-20:26 UTC). No user has more than 3 impressions, 3 bid requests or 1 site, so with the required thresholds all three audiences are empty. The tests cover every rule on synthetic data, including the boundaries.

`userIfa` is empty in the sample. The IFA appears only inside the `rawRequest` JSON of `request` events, which the audiences do not use, so every user resolves to `ip_ua`. In production `userIfa` should be filled at logging time: parsing the request JSON for every bid request is the most expensive thing this job could do.

## Scaling

**10× more data.**
- Partition the raw log by date and hour, so files are pruned instead of row groups. Aim for 256-512 MB files.
- AQE, on by default in Spark 3.5, sizes shuffle partitions. Map-side partial aggregation absorbs hot keys such as carrier NAT IPs.
- Filter invalid traffic, such as bots and data-center IPs, before the aggregation.
- Keep the user-day table instead of rebuilding it (see incremental updates).

**1 billion events per hour** (about 280k events per second).
- Aggregate continuously: Structured Streaming from Kafka with a watermark, counters per user and hour written to a Delta or Iceberg table partitioned by date and hour.
- The daily audience job then reads only these aggregates. Bid requests are the bulk of the volume, and the heavy `rawRequest` column stays out of this path.
- Hash the user key at ingestion and partition Kafka by it, so load spreads evenly.

**Incremental updates.**
- Store `UserDaily` as a table partitioned by date. Each run computes only the new day from that day's raw logs and overwrites that partition, so reruns are idempotent. Late data is handled by recomputing the last N days.
- B and C become a rollup of 7 small partitions instead of 7 days of raw logs. A reads its fixed window from the same table.
- For activation in the DSP, emit the difference from the previous run (users added and removed) via an anti-join instead of the full list.
