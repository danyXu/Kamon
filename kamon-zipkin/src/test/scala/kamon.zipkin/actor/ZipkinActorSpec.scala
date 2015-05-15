package kamon.zipkin.actor

import akka.actor.Props
import akka.testkit._
import kamon.testkit.BaseKamonSpec
import kamon.trace.{ HierarchyConfig, SegmentInfo, TraceInfo }
import kamon.util.{ NanoInterval, NanoTimestamp }
import kamon.zipkin.ZipkinConfig
import kamon.zipkin.models._

import scala.concurrent.duration._

class ZipkinActorSpec extends BaseKamonSpec("zipkin-actor-spec") {

  lazy val zipkinConfig = new ZipkinConfig(system.settings.config)

  "the zipkin actor system" should {

    "return a SpanBlock when the delay has been reached" in new TraceInfoTest {
      val prob = TestProbe()

      TestActorRef(Props(new ZipkinActorSupervisor(prob.ref))) ! trace

      prob.expectMsgType[SpanBlock](zipkinConfig.scheduler + 200.millis)
    }

    "not return anything after the delay has been reached" in new TraceInfoTest {
      val prob = TestProbe()
      val supervisor = TestActorRef(Props(new ZipkinActorSupervisor(prob.ref)))

      supervisor ! trace
      prob.expectMsgType[SpanBlock](zipkinConfig.scheduler + 200.millis)
      supervisor ! trace.copy(name = "root")

      prob.expectNoMsg(500 millis)
    }

    "create an unique actor for each trace" in new TraceInfoTest {
      val prob = TestProbe()
      val supervisor = TestActorRef(new ZipkinActorSupervisor(prob.ref))

      supervisor ! trace
      supervisor ! trace
      supervisor ! trace.copy(metadata = Map[String, String](HierarchyConfig.rootToken -> "racine"))
      supervisor ! prob.ref

      supervisor.underlyingActor.tokenActors.keySet should be(Set("racine", "root"))
    }

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

}