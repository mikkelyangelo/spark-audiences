package audiences

import java.time.LocalDate

// days and hours are in the session time zone
final case class Rules(
    impressionsFrom: LocalDate = LocalDate.of(2026, 7, 5),
    impressionsTo: LocalDate = LocalDate.of(2026, 7, 11),
    minImpressions: Long = 5,
    lookbackDays: Int = 7,
    bidRequestsAbove: Long = 50,
    minSites: Int = 3,
    countries: Seq[String] = Seq("USA", "CAN"),
    hoursFrom: Int = 18, // inclusive
    hoursTo: Int = 23, // exclusive: 18:00-22:59
    minDailyBidRequests: Long = 10
) {
  require(lookbackDays > 0 && minSites > 0, "lookbackDays and minSites must be positive")
  require(0 <= hoursFrom && hoursFrom < hoursTo && hoursTo <= 24, "expected 0 <= hoursFrom < hoursTo <= 24")

  def lookback(runDate: LocalDate): (LocalDate, LocalDate) = (runDate.minusDays(lookbackDays - 1L), runDate)
}
