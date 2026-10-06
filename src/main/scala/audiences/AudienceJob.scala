package audiences

import java.sql.Timestamp
import java.time.{Instant, LocalDate, ZoneId}

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

final case class JobConfig(
    input: String,
    output: String,
    runDate: LocalDate,
    timeZone: ZoneId = ZoneId.of("UTC"),
    outputFiles: Int = 1,
    rules: Rules = Rules()
)

object JobConfig {
  val Usage: String =
    "--input <path> --output <path> --run-date <yyyy-MM-dd> [--time-zone UTC] [--output-files 1] " +
      "[--impressions-from 2026-07-05] [--impressions-to 2026-07-11]"

  def parse(args: Seq[String]): JobConfig = {
    require(args.length % 2 == 0, s"expected --key value pairs. Usage: $Usage")
    val opts = args.grouped(2).map { case Seq(k, v) => k.stripPrefix("--") -> v }.toMap
    val unknown = opts.keySet -- Set("input", "output", "run-date", "time-zone", "output-files", "impressions-from", "impressions-to")
    require(unknown.isEmpty, s"unknown options ${unknown.mkString(", ")}. Usage: $Usage")
    def required(key: String): String = opts.getOrElse(key, throw new IllegalArgumentException(s"--$key is required. Usage: $Usage"))

    val defaults = Rules()
    JobConfig(
      input = required("input"),
      output = required("output"),
      runDate = LocalDate.parse(required("run-date")),
      timeZone = opts.get("time-zone").map(ZoneId.of).getOrElse(ZoneId.of("UTC")),
      outputFiles = opts.get("output-files").map(_.toInt).getOrElse(1),
      rules = defaults.copy(
        impressionsFrom = opts.get("impressions-from").map(LocalDate.parse).getOrElse(defaults.impressionsFrom),
        impressionsTo = opts.get("impressions-to").map(LocalDate.parse).getOrElse(defaults.impressionsTo)
      )
    )
  }
}

object AudienceJob {

  def main(args: Array[String]): Unit = {
    val config = JobConfig.parse(args.toSeq)
    val spark = SparkSession
      .builder()
      .appName("audiences")
      .config("spark.sql.session.timeZone", config.timeZone.getId)
      .getOrCreate()
    try run(spark, config)
    finally spark.stop()
  }

  def run(spark: SparkSession, config: JobConfig): Unit = {
    val rules = config.rules
    val generatedAt = lit(Timestamp.from(Instant.now()))

    val raw = spark.read.parquet(config.input)
    val daily = UserDaily
      .build(raw, Seq((rules.impressionsFrom, rules.impressionsTo), rules.lookback(config.runDate)), rules)
      .persist(StorageLevel.MEMORY_AND_DISK)

    try {
      val audiences = Seq(
        "A" -> Audiences.a(daily, rules),
        "B" -> Audiences.b(daily, config.runDate, rules),
        "C" -> Audiences.c(daily, config.runDate, rules)
      )
      for ((audienceId, users) <- audiences) {
        users
          .select(col("user_id"), col("user_id_type"), lit(audienceId).as("audience_id"), generatedAt.as("generated_at"))
          .repartition(config.outputFiles)
          .write
          .mode(SaveMode.Overwrite)
          .parquet(s"${config.output}/audience_$audienceId")
      }
    } finally daily.unpersist()
  }
}
