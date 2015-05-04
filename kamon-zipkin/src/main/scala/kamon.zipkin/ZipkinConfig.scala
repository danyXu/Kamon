package kamon.zipkin

import java.net.InetAddress

import com.typesafe.config.Config

object ZipkinConfig {
  val internalPrefix = "internal."

  val spanClass = internalPrefix + "class"
  val spanType = internalPrefix + "type"
  val spanUniqueClass = internalPrefix + "uniqueClass"

  val segmentBegin = "BEGIN> "
  val segmentEnd = "END> "

  val endpointMarker = "EndpointWriter"

  val remote = internalPrefix + "remote"
}

class ZipkinConfig(config: Config) {
  val recordMinDuration = config.getInt("kamon.zipkin.record-min")

  object service {
    val host = config.getString("app.host") match {
      case "auto" ⇒ InetAddress.getLocalHost
      case name   ⇒ InetAddress.getByName(name)
    }
    val port = config.getInt("app.port")
  }
}