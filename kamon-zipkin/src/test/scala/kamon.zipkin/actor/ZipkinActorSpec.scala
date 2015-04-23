package kamon.zipkin.actor
import java.net.InetAddress
import java.nio.ByteBuffer

import akka.actor.Props
import akka.testkit.{ TestActorRef, TestProbe }
import kamon.akka.remote.RemoteConfig
import kamon.testkit.BaseKamonSpec
import com.github.levkhomich.akka.tracing.thrift
import kamon.trace.{ SegmentInfo, TraceInfo }
import kamon.util.{ NanoInterval, NanoTimestamp }
import kamon.zipkin.ZipkinConfig

import scala.collection.mutable

class ZipkinActorSpec extends BaseKamonSpec("zipkin-instrumentation-spec") {

  "the Kamon Zipkin module" should {
    "create an Endpoint correctly" in new EndpointFixture {
      val e = new thrift.Endpoint()
      e.set_service_name(service)
      e.set_ipv4(ByteBuffer.wrap(InetAddress.getByName(host).getAddress).getInt)
      e.set_port(port.toShort)

      e should be(endpoint)
    }
    "create a Span correctly" in new SpanFixture {
      val s = new thrift.Span()
      s.set_trace_id(0L)
      s.set_name(spanName)
      s.set_id(spanId)
      s.set_parent_id(parentSpanId)

      val sa = new thrift.Annotation()
      sa.set_timestamp(spanStart)
      sa.set_value(thrift.zipkinConstants.SERVER_RECV)
      sa.set_host(endpoint)
      val ea = new thrift.Annotation()
      ea.set_timestamp(spanStart + spanDuration)
      ea.set_value(thrift.zipkinConstants.SERVER_SEND)
      ea.set_host(sa.get_host())

      val begin = new thrift.Annotation()
      begin.set_timestamp(spanStart)
      begin.set_value(ZipkinConfig.segmentBegin + spanName)
      val end = new thrift.Annotation()
      end.set_timestamp(spanStart + spanDuration)
      end.set_value(ZipkinConfig.segmentEnd + spanName)

      s.add_to_annotations(sa)
      s.add_to_annotations(begin)
      s.add_to_annotations(end)
      s.add_to_annotations(ea)

      s should be(span)
    }

    "return a SpanBlock when the TraceInfo concerns the rootToken" in {
      val prob = TestProbe()
      val zipkinActor = TestActorRef(Props(new ZipkinActor(prob.ref)))
      prob.watch(zipkinActor)

      val trace = TraceInfo("", "root", NanoTimestamp.now, NanoInterval.default,
        Map[String, String](RemoteConfig.rootToken -> "root", RemoteConfig.parentToken -> "root"), List[SegmentInfo]())
      zipkinActor ! trace

      prob.expectMsgType[SpanBlock]
    }

    "not return anything when the TraceInfo doesn't concern the rootToken" in new TraceInfoFixture {
      val prob = TestProbe()
      val zipkinActor = TestActorRef(Props(new ZipkinActor(prob.ref)))
      prob.watch(zipkinActor)

      zipkinActor ! trace

      prob.expectNoMsg()
    }
  }

  trait EndpointFixture {
    val service = "test"
    val host = "localhost"
    val port = 8000

    val endpoint = Endpoint.createEndpoint(service, host, port)
  }

  trait TraceInfoFixture {
    val traceName = "traceName"
    val traceToken = "traceToken"
    val traceStart = NanoTimestamp.now
    val traceDuration = NanoInterval.default
    val metadata = Map[String, String]()
    val segments = List[SegmentInfo]()

    val trace = TraceInfo(traceName, traceToken, traceStart, traceDuration, metadata, segments)
  }

  trait SpanFixture extends TraceInfoFixture with EndpointFixture {
    val traceId = 0L
    val spanId = 0L
    val parentSpanId = 0L
    val spanName = "spanName"
    val spanStart = NanoTimestamp.now.nanos
    val spanDuration = NanoInterval.default.nanos

    val span = new Span(trace, traceId, spanId, spanName, spanStart, spanDuration, mutable.Map[String, String](), parentSpanId, endpoint).simpleSpan
  }

}