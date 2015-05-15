package kamon.zipkin.models

import java.net.InetAddress
import java.nio.ByteBuffer

import com.github.levkhomich.akka.tracing.thrift
import kamon.testkit.BaseKamonSpec
import kamon.trace.{ TraceInfo, SegmentInfo }
import kamon.util.{ NanoInterval, NanoTimestamp }
import kamon.zipkin.ZipkinConfig

import scala.collection.mutable

class ModelsSpec extends BaseKamonSpec("zipkin-models-spec") {

  "the models classes" should {

    "create an Endpoint correctly" in new EndpointTest {
      val e = new thrift.Endpoint()
        .set_service_name(service)
        .set_ipv4(ByteBuffer.wrap(InetAddress.getByName(host).getAddress).getInt)
        .set_port(port.toShort)

      e should be(endpoint)
    }

    "create a Span correctly" in new SpanTest {
      val s = new thrift.Span()
        .set_trace_id(0L)
        .set_id(0L)
        .set_parent_id(0L)
        .set_name(spanName)

      val sa = new thrift.Annotation(spanStart, thrift.zipkinConstants.SERVER_RECV).set_host(endpoint)
      val ea = new thrift.Annotation(spanStart + spanDuration, thrift.zipkinConstants.SERVER_SEND).set_host(sa.get_host())
      val begin = new thrift.Annotation(spanStart, ZipkinConfig.segmentBegin + spanName)
      val end = new thrift.Annotation(spanStart + spanDuration, ZipkinConfig.segmentEnd + spanName)

      s.add_to_annotations(sa)
      s.add_to_annotations(begin)
      s.add_to_annotations(end)
      s.add_to_annotations(ea)

      s should be(span)
    }

  }

  trait EndpointTest {
    val service = "test"
    val host = "localhost"
    val port = 8000

    val endpoint = Endpoint.createEndpoint(service, host, port)
  }

  trait TraceInfoTest {
    val traceName = "traceName"
    val traceToken = "traceToken"
    val traceStart = NanoTimestamp.now
    val traceDuration = NanoInterval.default

    val trace = TraceInfo(traceName, traceToken, traceStart, traceDuration, Map[String, String](), List[SegmentInfo]())
  }

  trait SpanTest extends TraceInfoTest with EndpointTest {
    val traceId = ""
    val spanId = ""
    val parentSpanId = ""
    val spanName = "spanName"
    val spanStart = NanoTimestamp.now.nanos
    val spanDuration = NanoInterval.default.nanos

    val span = new Span(trace, traceId, spanId, spanName, spanStart, spanDuration, mutable.Map[String, String](), parentSpanId, endpoint).simpleSpan
  }

}
