package aliaksei.darapiyevich.impression.enrichment

import aliaksei.darapiyevich.BaseArgsParser
import org.rogach.scallop.ScallopOption

class ImpressionEventAppArgsParser(arguments: Seq[String]) extends BaseArgsParser(arguments) {
  private lazy val sessionExpirationThresholdSecondsOpt: ScallopOption[Int] = opt[Int](
    name = "session-expiration-threshold",
    descr = "Number of seconds to consider a session as inactive",
    default = Some(ImpressionEventAppArgsParser.DefaultSessionExpirationThresholdSeconds),
    argName = "e",
    noshort = true
  )
  verify()

  def sessionExpirationThresholdSeconds: Int = {
    sessionExpirationThresholdSecondsOpt()
  }
}

object ImpressionEventAppArgsParser {
  val DefaultSessionExpirationThresholdSeconds: Int = 5 * 60
}
