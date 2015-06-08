package kamon.zipkin

import java.net.InetAddress

import com.typesafe.config.Config
import kamon.trace.HierarchyConfig

object ZipkinConfig {
  val internalPrefix = HierarchyConfig.internalPrefix

  val parentClass = internalPrefix + "parentClass"
  val spanClass = internalPrefix + "class"
  val spanType = internalPrefix + "type"
  val remote = internalPrefix + "remote"

  val segmentBegin = "BEGIN> "
  val segmentEnd = "END> "

  val rootName = "Request"
  val endpointMarker = "EndpointWriter"
}

class ZipkinConfig(config: Config) {
  import scala.concurrent.duration._

  val recordMinDuration = config.getInt("kamon.zipkin.record-min")
  val scheduler = config.getInt("kamon.zipkin.submit-scheduler") millis
  val recordParamsValue = config.getBoolean("kamon.zipkin.record-params")

  object service {
    val host = config.getString("app.host") match {
      case "auto" ⇒ InetAddress.getLocalHost
      case name   ⇒ InetAddress.getByName(name)
    }
    val port = config.getInt("app.port")
  }
}