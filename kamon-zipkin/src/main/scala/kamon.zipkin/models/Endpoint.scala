package kamon.zipkin.models

import java.net.InetAddress
import java.nio.ByteBuffer

import com.github.levkhomich.akka.tracing.thrift

object Endpoint {
  def createApplicationEndpoint(service: String) =
    createEndpoint(service, "localhost", 0)

  def createEndpoint(service: String, host: String, port: Int): thrift.Endpoint =
    createEndpoint(service, InetAddress.getByName(host), port)

  def createEndpoint(service: String, host: InetAddress, port: Int): thrift.Endpoint =
    createEndpoint(service, ByteBuffer.wrap(host.getAddress).getInt, port.toShort)

  def createEndpoint(service: String, host: Int, port: Short): thrift.Endpoint = {
    val e = new thrift.Endpoint()
    e.set_service_name(service)
    e.set_ipv4(host)
    e.set_port(port)
    e
  }
}