package kamon.zipkin.actor

import java.net.InetAddress
import java.nio.ByteBuffer
import akka.actor.Props
import akka.testkit.{ TestActorRef, TestProbe }
import kamon.testkit.BaseKamonSpec
import com.github.levkhomich.akka.tracing.thrift
import kamon.trace.{ HierarchyConfig, SegmentInfo, TraceInfo }
import kamon.util.{ NanoInterval, NanoTimestamp }
import kamon.zipkin.ZipkinConfig
import kamon.zipkin.models._

import scala.collection.mutable
import scala.concurrent.duration._

class ZipkinActorSpec extends BaseKamonSpec("zipkin-instrumentation-spec") {

  "the zipkin actor system" should {

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

    "return a SpanBlock when the TraceInfo concerns the rootToken" in new TraceInfoTest {
      val prob = TestProbe()
      TestActorRef(Props(new ZipkinActorSupervisor(prob.ref))) ! trace.copy(token = trace.metadata(HierarchyConfig.rootToken))
      prob.expectMsgType[SpanBlock]
    }

    "not return anything when the TraceInfo doesn't concern the rootToken" in new TraceInfoTest {
      val prob = TestProbe()
      TestActorRef(Props(new ZipkinActorSupervisor(prob.ref))) ! trace
      prob.expectNoMsg(500 millis)
    }

    "create an unique actor for each trace" in new TraceInfoTest {
      val prob = TestProbe()
      val supervisor = TestActorRef(Props(new ZipkinActorSupervisor(self)))

      supervisor ! trace
      supervisor ! prob.ref

      prob.expectMsg(List("root"))
    }

    /*
    "delete an actor when its corresponding trace ended" in new TraceInfoTest {
      val prob = TestProbe()
      val supervisor = TestActorRef(Props(new ZipkinActorSupervisor(self)))

      supervisor ! trace
      supervisor ! trace.copy(token = trace.metadata(HierarchyConfig.rootToken))
      prob.expectNoMsg(3000 millis)
      supervisor ! prob.ref

      prob.expectMsg(List())
    }
    */

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
    val metadata = Map[String, String](HierarchyConfig.rootToken -> "root")
    val segments = List[SegmentInfo]()

    val trace = TraceInfo(traceName, traceToken, traceStart, traceDuration, metadata, segments)
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