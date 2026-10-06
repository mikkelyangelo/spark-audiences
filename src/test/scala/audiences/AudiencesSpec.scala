package audiences

import java.nio.file.Files
import java.time.{LocalDate, LocalDateTime, ZoneOffset}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

final case class RawEvent(
    time: Long,
    siteType: String,
    userIfa: Option[String],
    userIp: Option[String],
    userUa: Option[String],
    ip: Option[String],
    ipv6: Option[String],
    userAgent: Option[String],
    countryIso3: String,
    site: Option[Long],
    bundle: Option[String],
    domain: Option[String],
    impr: Option[Long],
    click: Option[Long],
    bidRequest: Option[Long]
)

class AudiencesSpec extends AnyFunSuite with BeforeAndAfterAll {

  private lazy val spark = SparkSession
    .builder()
    .master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  override def afterAll(): Unit = spark.stop()

  private val rules = Rules()
  private val runDate = LocalDate.of(2026, 7, 11)
  private val lastWeek = (5 to 11).map(d => LocalDate.of(2026, 7, d))

  private sealed trait User
  private final case class Web(ip: String) extends User
  private final case class App(ifa: String) extends User

  private def event(user: User, at: LocalDateTime, country: String = "USA", site: String = "a.com",
                    impr: Long = 0, click: Long = 0, bidRequest: Long = 0): RawEvent = {
    val millis = at.toInstant(ZoneOffset.UTC).toEpochMilli
    val (siteType, ifa, ip) = user match {
      case Web(ip) => ("Site", None, ip)
      case App(ifa) => ("App", Some(ifa), "10.0.0.1")
    }
    def opt(n: Long) = if (n == 0) None else Some(n)
    RawEvent(millis, siteType, ifa, None, None, Some(ip), None, Some("Mozilla/5.0"), country, None,
      if (siteType == "App") Some(site) else Some(""), Some(site), opt(impr), opt(click), opt(bidRequest))
  }

  private def at(day: LocalDate, hour: Int): LocalDateTime = day.atTime(hour, 30)

  private def daily(events: Seq[RawEvent]): DataFrame =
    UserDaily.build(events.toDF(), Seq((rules.impressionsFrom, rules.impressionsTo), rules.lookback(runDate)), rules)

  private def ids(audience: DataFrame): Set[String] = audience.as[(String, String)].collect().map(_._1).toSet

  private def webId(ip: String): String = md5(s"$ip|Mozilla/5.0")
  private def md5(s: String): String =
    java.security.MessageDigest.getInstance("MD5").digest(s.getBytes("UTF-8")).map("%02x".format(_)).mkString

  test("identity: device id for apps, IP + User-Agent for web and for apps without a usable IFA") {
    val day = at(runDate, 12)
    val users = UserDaily
      .build(
        Seq(
          event(App("6B6C99DB-6A77-4602-9E88-199E33787006"), day, impr = 1),
          event(App("00000000-0000-0000-0000-000000000000"), day, impr = 1),
          event(Web("1.1.1.1"), day, impr = 1),
          event(Web("1.1.1.1"), day, impr = 1).copy(userAgent = Some(" "))
        ).toDF(),
        Seq((runDate, runDate)),
        rules
      )
      .select("user_id", "user_id_type")
      .as[(String, String)]
      .collect()
      .toSet

    assert(users == Set(
      md5("6b6c99db-6a77-4602-9e88-199e33787006") -> Identity.DeviceId,
      md5("10.0.0.1|Mozilla/5.0") -> Identity.IpUserAgent,
      webId("1.1.1.1") -> Identity.IpUserAgent
    ))
  }

  test("A: at least 5 impressions on 5-11 Jul and no clicks") {
    val inside = LocalDate.of(2026, 7, 8)
    val events =
      Seq.fill(5)(event(Web("1.0.0.1"), at(inside, 10), impr = 1)) ++
        Seq.fill(4)(event(Web("1.0.0.2"), at(inside, 10), impr = 1)) ++
        Seq.fill(5)(event(Web("1.0.0.3"), at(inside, 10), impr = 1)) :+ event(Web("1.0.0.3"), at(inside, 11), click = 1)
    val outside = Seq.fill(4)(event(Web("1.0.0.4"), at(inside, 10), impr = 1)) :+
      event(Web("1.0.0.4"), at(LocalDate.of(2026, 7, 12), 1), impr = 1)

    assert(ids(Audiences.a(daily(events ++ outside), rules)) == Set(webId("1.0.0.1")))
  }

  test("B: more than 50 bid requests from at least 3 sites in the last 7 days") {
    def requests(ip: String, n: Int, sites: Seq[String], day: LocalDate = runDate) =
      (0 until n).map(i => event(Web(ip), at(day, 10), site = sites(i % sites.size), bidRequest = 1))
    val events =
      requests("2.0.0.1", 51, Seq("a.com", "b.com", "c.com")) ++
        requests("2.0.0.2", 50, Seq("a.com", "b.com", "c.com")) ++
        requests("2.0.0.3", 51, Seq("a.com", "b.com")) ++
        // spread over days: no single day has 3 sites, the week does
        requests("2.0.0.4", 30, Seq("a.com", "b.com"), lastWeek.head) ++ requests("2.0.0.4", 21, Seq("c.com")) ++
        // 8 days ago is outside the window
        requests("2.0.0.5", 50, Seq("a.com", "b.com", "c.com")) ++ requests("2.0.0.5", 1, Seq("d.com"), runDate.minusDays(7))

    assert(ids(Audiences.b(daily(events), runDate, rules)) == Set(webId("2.0.0.1"), webId("2.0.0.4")))
  }

  test("C: 10+ bid requests from 18:00 to 23:00 in US or Canada on each of the last 7 days") {
    def evenings(ip: String, country: String = "USA", hour: Int = 18, perDay: LocalDate => Int = _ => 10) =
      lastWeek.flatMap(d => Seq.fill(perDay(d))(event(Web(ip), at(d, hour), country = country, bidRequest = 1)))
    val events =
      evenings("3.0.0.1") ++
        evenings("3.0.0.2", country = "CAN", hour = 22) ++
        evenings("3.0.0.3", perDay = d => if (d == runDate) 9 else 10) ++
        evenings("3.0.0.4", country = "DEU") ++
        evenings("3.0.0.5", hour = 23) ++
        evenings("3.0.0.6", hour = 17)

    assert(ids(Audiences.c(daily(events), runDate, rules)) == Set(webId("3.0.0.1"), webId("3.0.0.2")))
  }

  test("job writes one Parquet file per audience with the output schema") {
    val input = Files.createTempDirectory("raw").resolve("events").toString
    val output = Files.createTempDirectory("audiences").toString
    Seq.fill(5)(event(Web("4.0.0.1"), at(runDate, 12), impr = 1)).toDF().write.parquet(input)

    AudienceJob.run(spark, JobConfig(input, output, runDate))

    for (id <- Seq("A", "B", "C")) {
      val dir = new java.io.File(s"$output/audience_$id")
      assert(dir.listFiles().count(_.getName.endsWith(".parquet")) == 1)
      val result = spark.read.parquet(dir.getPath)
      assert(result.columns.toSeq == Seq("user_id", "user_id_type", "audience_id", "generated_at"))
      assert(result.count() == (if (id == "A") 1 else 0))
    }
  }
}
