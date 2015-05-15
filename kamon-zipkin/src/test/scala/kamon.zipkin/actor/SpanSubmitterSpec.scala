package kamon.zipkin.actor

import akka.testkit.TestActorRef
import kamon.testkit.BaseKamonSpec
import kamon.trace.HierarchyConfig
import kamon.util.NanoTimestamp
import kamon.zipkin.ZipkinConfig
import kamon.zipkin.models.{ Span, SpanBlock, SpanTest }
import scala.collection.mutable

class SpanSubmitterSpec extends BaseKamonSpec("zipkin-actor-spec") {

  "the span submitter" should {

    "submit spans correctly" in new SpanTest {
      val spanSubmitter = TestActorRef(new SpanSubmitter())

      val sb = new SpanBlock(mutable.Map[String, Span](traceToken -> span), traceToken, false)
      spanSubmitter ! sb
      expectMsg(Map[String, Span](traceToken -> span))

      spanSubmitter ! sb
      expectMsg(Map[String, Span](traceToken -> span))
    }

    "merge spans corresponding to same instance" in new SpanTest {
      val spanSubmitter = TestActorRef(new SpanSubmitter())

      val spanA1 = span.copy(trace = trace.copy(metadata = Map[String, String](HierarchyConfig.spanUniqueClass -> "sameInstance", ZipkinConfig.spanType -> "span1")))
      val spanA2 = span.copy(trace = trace.copy(metadata = Map[String, String](HierarchyConfig.spanUniqueClass -> "sameInstance", ZipkinConfig.spanType -> "span2"),
        timestamp = new NanoTimestamp(NanoTimestamp.now.nanos + 100L)))
      val spanA3 = span.copy(trace = trace.copy(metadata = Map[String, String](HierarchyConfig.spanUniqueClass -> "sameInstance", ZipkinConfig.spanType -> "span3"),
        timestamp = new NanoTimestamp(NanoTimestamp.now.nanos + 200L)))

      val spanB1 = span.copy(trace = trace.copy(metadata = Map[String, String](HierarchyConfig.spanUniqueClass -> "anotherInstance", ZipkinConfig.spanType -> "span4"),
        timestamp = new NanoTimestamp(NanoTimestamp.now.nanos + 300L)))
      val spanB2 = span.copy(trace = trace.copy(metadata = Map[String, String](HierarchyConfig.spanUniqueClass -> "anotherInstance", ZipkinConfig.spanType -> "span5"),
        timestamp = new NanoTimestamp(NanoTimestamp.now.nanos + 400L)))

      val sb = new SpanBlock(mutable.Map[String, Span]("token3" -> spanA3, traceToken -> spanA1, "token2" -> spanA2, "token4" -> spanB1, "token5" -> spanB2), traceToken)
      spanSubmitter ! sb

      expectMsg(Map[String, Span](traceToken -> spanA1, "token4" -> spanB1))
    }

    "give to each span correct parent" in new SpanTest {
      val spanSubmitter = TestActorRef(new SpanSubmitter())

      val realParent = span.copy(trace = trace.copy(metadata = Map[String, String](HierarchyConfig.spanUniqueClass -> "parent", ZipkinConfig.spanType -> "span1")))
      val startParent = span.copy(trace = trace.copy(metadata = Map[String, String](HierarchyConfig.spanUniqueClass -> "parent", ZipkinConfig.spanType -> "span2"),
        timestamp = new NanoTimestamp(NanoTimestamp.now.nanos + 100L)), spanId = "anotherToken")
      val child = span.copy(trace = trace.copy(metadata = Map[String, String](ZipkinConfig.spanType -> "span3", ZipkinConfig.parentClass -> "parent"),
        timestamp = new NanoTimestamp(NanoTimestamp.now.nanos + 200L)), spanId = "child", parentSpanId = "anotherToken")

      val sb = new SpanBlock(mutable.Map[String, Span](traceToken -> realParent, "token2" -> startParent, "token3" -> child), traceToken)
      spanSubmitter ! sb

      expectMsg(Map[String, Span](traceToken -> realParent, "token3" -> child.copy(parentSpanId = realParent.spanId)))
    }

  }

}