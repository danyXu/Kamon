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

class ZipkinConfig(zipkinConfig: Config) {
  object service {
    private val config = zipkinConfig.getConfig("service")

    val host = config.getString("host") match {
      case "auto" ⇒ InetAddress.getLocalHost
      case host   ⇒ InetAddress.getByName(host)
    }
    val port = config.getInt("port")
  }
}