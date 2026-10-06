package audiences

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object Identity {
  val DeviceId = "device_id"
  val IpUserAgent = "ip_ua"

  private[audiences] def nonEmpty(name: String): Column = {
    val value = trim(col(name))
    when(value =!= "", value)
  }

  private val ifa = lower(nonEmpty("userIfa"))
  // limit ad tracking sends a zeroed IFA, the same for every such device
  private val hasIfa = ifa.isNotNull && !ifa.rlike("^[0-]+$")
  private val ip = coalesce(nonEmpty("userIp"), nonEmpty("ip"), nonEmpty("ipv6"))
  private val userAgent = coalesce(nonEmpty("userUa"), nonEmpty("userAgent"))

  val userIdType: Column =
    when(col("siteType") === "App" && hasIfa, DeviceId)
      .when(ip.isNotNull && userAgent.isNotNull, IpUserAgent)

  // an IP never contains "|", so the separator is unambiguous
  val userId: Column =
    when(userIdType === DeviceId, md5(ifa))
      .when(userIdType === IpUserAgent, md5(concat_ws("|", ip, userAgent)))
}
