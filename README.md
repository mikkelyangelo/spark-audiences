# spark-audiences

Builds audiences A, B and C from raw DSP logs. Output is one Parquet dataset per audience with `user_id`, `user_id_type`, `audience_id`, `generated_at`.

Scala 2.12, Spark 3.5, Java 17.

## Run

```bash
sbt test
sbt package
spark-submit --class audiences.AudienceJob target/scala-2.12/spark-audiences_2.12-0.1.0.jar \
  --input s3://bucket/rawlog/ --output s3://bucket/audiences/ --run-date 2026-07-11
```

Locally: `sbt "run --input data --output out --run-date 2026-07-11"`.

Options:

- `--run-date` (required): last day of the 7-day window
- `--time-zone`, default `UTC`: zone for days and hours
- `--output-files`, default `1`: files per audience
- `--impressions-from`, `--impressions-to`, default `2026-07-05` and `2026-07-11`: window for audience A

Results go to `<output>/audience_A`, `audience_B` and `audience_C` and are overwritten on rerun.

## Design

The job reads the log once and aggregates it by user and day: impressions, clicks, bid requests, bid requests from US/CA between 18:00 and 23:00, and up to 3 distinct sites. This table is cached, and each audience is a small aggregation over it.

The scan reads 15 of 122 columns. The date window is applied to the raw `time` column, so Parquet skips row groups outside it.

Per-day site sets are capped at 3. Their union has at least 3 sites exactly when the full union does, so a user with thousands of sites costs no more memory than one with three.

Output uses `repartition`, not `coalesce(1)`, so the aggregation itself stays parallel.

## User identity

- In-app traffic with an advertising ID: `device_id`, `md5(lower(ifa))`.
- Web traffic, and apps where the IFA is empty or zeroed (limit ad tracking): `ip_ua`, `md5(ip + "|" + user_agent)`.

IP comes from `userIp`, `ip` or `ipv6`, User-Agent from `userUa` or `userAgent`. Rows with no usable key are skipped.

## Assumptions

The task leaves some points open. This is how I read them:

- A: impressions are `impr` events. 5-11 Jul is 2026, the year of the data. "Never clicked" means no clicks in the same window.
- B: "more than 50" is `> 50`. A site is the site ID, else the app bundle, else the domain (site ID is empty in the sample).
- C: the country is checked per request. "From 18 to 23" is 18:00-22:59. The 10 requests must be there on each of the 7 days.
- "Last 7 days" is the run date and the 6 days before it, in `--time-zone`. Per-user local time would need a geo to time zone lookup, and region is empty in the sample.
- Events are not deduplicated. If logging is at-least-once, I would deduplicate by `(requestId, impId, event)` first.

## Sample data

The sample covers 4 minutes of 11 Jul 2026, 957 rows. No user has more than 3 impressions or 3 bid requests, so all three audiences come out empty. The rules are checked in tests on generated data, including the boundaries.

`userIfa` is empty in the sample. The IFA is only inside the `rawRequest` JSON of `request` events, which these audiences don't use, so all users resolve to `ip_ua`. It is better to fill `userIfa` when the event is logged than to parse JSON for every bid request.

## Discussion questions

10x more data: partition the log by date and hour and keep files at a few hundred MB, filter bots and data-center IPs before aggregating, keep the user-day table instead of rebuilding it. AQE already sizes shuffle partitions, and partial aggregation handles hot keys like carrier NAT IPs.

1 billion events per hour (~280k/s): aggregate continuously with Structured Streaming from Kafka into per-user hourly counters in Delta or Iceberg. The daily job reads those counters, not the raw log, and the heavy `rawRequest` column never enters this path.

Incremental updates: store the user-day table partitioned by date. Each run computes only the new day and overwrites that partition, and late data is handled by recomputing the last few days. B and C then read 7 small partitions. For activation, send only users added and removed since the previous run.
