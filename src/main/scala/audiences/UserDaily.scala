package audiences

import java.time.{LocalDate, ZoneId}

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

object UserDaily {
  val UserColumns: Seq[String] = Seq("user_id", "user_id_type")

  def build(raw: DataFrame, windows: Seq[(LocalDate, LocalDate)], rules: Rules): DataFrame = {
    val zone = ZoneId.of(raw.sparkSession.conf.get("spark.sql.session.timeZone"))
    def millis(day: LocalDate): Long = day.atStartOfDay(zone).toInstant.toEpochMilli
    // a range on the raw column is pushed down to Parquet, a filter on the derived date is not
    val inWindows = windows
      .map { case (from, to) => col("time") >= millis(from) && col("time") < millis(to.plusDays(1)) }
      .reduce(_ || _)

    def counter(name: String): Column = coalesce(col(name), lit(0L))
    val impressions = counter("impr")
    val clicks = counter("click")
    val bidRequests = counter("bidRequest")

    val ts = timestamp_millis(col("time"))
    val eveningInCountries = hour(ts) >= rules.hoursFrom && hour(ts) < rules.hoursTo &&
      col("countryIso3").isin(rules.countries: _*)
    val site = coalesce(col("site").cast("string"), Identity.nonEmpty("bundle"), Identity.nonEmpty("domain"))

    raw
      .where(inWindows && (impressions > 0 || clicks > 0 || bidRequests > 0))
      .select(
        Identity.userId.as("user_id"),
        Identity.userIdType.as("user_id_type"),
        to_date(ts).as("date"),
        impressions.as("impressions"),
        clicks.as("clicks"),
        bidRequests.as("bid_requests"),
        when(eveningInCountries, bidRequests).otherwise(0L).as("evening_bid_requests"),
        when(bidRequests > 0, site).as("site")
      )
      .where(col("user_id").isNotNull)
      .groupBy("user_id", "user_id_type", "date")
      .agg(
        sum("impressions").as("impressions"),
        sum("clicks").as("clicks"),
        sum("bid_requests").as("bid_requests"),
        sum("evening_bid_requests").as("evening_bid_requests"),
        // the union of capped per-day sets reaches minSites exactly when the full union does
        slice(collect_set("site"), 1, rules.minSites).as("sites")
      )
  }
}
