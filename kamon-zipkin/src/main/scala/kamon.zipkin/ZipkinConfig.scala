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
  val recordMinDuration = 100

  val remote = internalPrefix + "remote"
}

class ZipkinConfig(config: Config) {
  object service {
    val host = config.getString("host") match {
      case "auto" ⇒ InetAddress.getLocalHost
      case name   ⇒ InetAddress.getByName(name)
    }
    val port = config.getInt("port")
  }
}