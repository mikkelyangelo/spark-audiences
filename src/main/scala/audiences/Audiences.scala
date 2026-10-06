package audiences

import java.time.LocalDate

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Audiences {
  import UserDaily.UserColumns

  private def between(daily: DataFrame, window: (LocalDate, LocalDate)): DataFrame =
    daily.where(col("date").between(window._1, window._2))

  def a(daily: DataFrame, rules: Rules): DataFrame =
    between(daily, (rules.impressionsFrom, rules.impressionsTo))
      .groupBy(UserColumns.map(col): _*)
      .agg(sum("impressions").as("impressions"), sum("clicks").as("clicks"))
      .where(col("impressions") >= rules.minImpressions && col("clicks") === 0)
      .select(UserColumns.map(col): _*)

  def b(daily: DataFrame, runDate: LocalDate, rules: Rules): DataFrame =
    between(daily, rules.lookback(runDate))
      .groupBy(UserColumns.map(col): _*)
      .agg(sum("bid_requests").as("bid_requests"), array_distinct(flatten(collect_list("sites"))).as("sites"))
      .where(col("bid_requests") > rules.bidRequestsAbove && size(col("sites")) >= rules.minSites)
      .select(UserColumns.map(col): _*)

  def c(daily: DataFrame, runDate: LocalDate, rules: Rules): DataFrame =
    between(daily, rules.lookback(runDate))
      .where(col("evening_bid_requests") >= rules.minDailyBidRequests)
      .groupBy(UserColumns.map(col): _*)
      .agg(count(lit(1)).as("days"))
      .where(col("days") === rules.lookbackDays)
      .select(UserColumns.map(col): _*)
}
