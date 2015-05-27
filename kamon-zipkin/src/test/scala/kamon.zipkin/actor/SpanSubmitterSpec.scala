package kamon.zipkin.actor

import akka.testkit.TestActorRef
import com.typesafe.config.ConfigFactory
import kamon.testkit.BaseKamonSpec
import kamon.trace.{ TraceSettings, HierarchyConfig }
import kamon.util.NanoTimestamp
import kamon.zipkin.ZipkinConfig
import kamon.zipkin.models.{ Span, SpanBlock, SpanTest }
import scala.collection.mutable
import scala.concurrent.duration._

class SpanSubmitterSpec extends BaseKamonSpec("zipkin-actor-spec") {

  "the span submitter" should {

    "submit spans correctly in normal mode" in new SpanTest {
      val spanSubmitter = TestActorRef(new SpanSubmitter(TraceSettings(system.settings.config)))

      val sb = new SpanBlock(mutable.Map[String, Span](traceToken -> span), traceToken)
      spanSubmitter ! sb
      expectMsg(Map[String, Span](traceToken -> span))

      spanSubmitter ! sb
      expectMsg(Map[String, Span](traceToken -> span))
    }

    "submit spans in threshold mode if the total duration is longer than minimum" in new SpanTest {
      val config = ConfigFactory.parseString(
        """
          |kamon {
          |  trace {
          |    level-of-detail = simple-trace
          |    sampling = threshold
          |    threshold-sampler {
          |      minimum-elapsed-time = 500 ms
          |    }
          |    token-name = ""
          |  }
          |}
        """.stripMargin)
      val spanSubmitter = TestActorRef(new SpanSubmitter(TraceSettings(config)))

      val span1 = span.copy(trace = trace.copy(metadata = Map[String, String](HierarchyConfig.spanUniqueClass -> "instance1", ZipkinConfig.spanType -> "span1")), duration = 600000L)
      val span2 = span.copy(trace = trace.copy(metadata = Map[String, String](HierarchyConfig.spanUniqueClass -> "instance2", ZipkinConfig.spanType -> "span2")), duration = 100000L)

      spanSubmitter ! new SpanBlock(mutable.Map[String, Span]("token1" -> span1, "token2" -> span2), traceToken)

      expectMsg(Map[String, Span]("token1" -> span1, "token2" -> span2))
    }

    "not submit spans in threshold mode if the total duration is shorter than minimum" in new SpanTest {
      val config = ConfigFactory.parseString(
        """
          |kamon {
          |  trace {
          |    level-of-detail = simple-trace
          |    sampling = threshold
          |    threshold-sampler {
          |      minimum-elapsed-time = 500 ms
          |    }
          |    token-name = ""
          |  }
          |}
        """.stripMargin)
      val spanSubmitter = TestActorRef(new SpanSubmitter(TraceSettings(config)))

      val span1 = span.copy(trace = trace.copy(metadata = Map[String, String](HierarchyConfig.spanUniqueClass -> "instance1", ZipkinConfig.spanType -> "span1")), duration = 300000L)
      val span2 = span.copy(trace = trace.copy(metadata = Map[String, String](HierarchyConfig.spanUniqueClass -> "instance2", ZipkinConfig.spanType -> "span2")), duration = 100000L)

      spanSubmitter ! new SpanBlock(mutable.Map[String, Span]("token1" -> span1, "token2" -> span2), traceToken)

      expectNoMsg(500 millis)
    }

    "merge spans corresponding to same instance" in new SpanTest {
      val spanSubmitter = TestActorRef(new SpanSubmitter(TraceSettings(system.settings.config)))

      val spanA1 = span.copy(trace = trace.copy(metadata = Map[String, String](HierarchyConfig.spanUniqueClass -> "sameInstance", ZipkinConfig.spanType -> "span1")))
      val spanA2 = span.copy(trace = trace.copy(metadata = Map[String, String](HierarchyConfig.spanUniqueClass -> "sameInstance", ZipkinConfig.spanType -> "span2"),
        timestamp = new NanoTimestamp(NanoTimestamp.now.nanos + 100L)))
      val spanA3 = span.copy(trace = trace.copy(metadata = Map[String, String](HierarchyConfig.spanUniqueClass -> "sameInstance", ZipkinConfig.spanType -> "span3"),
        timestamp = new NanoTimestamp(NanoTimestamp.now.nanos + 200L)))

      val spanB1 = span.copy(trace = trace.copy(metadata = Map[String, String](HierarchyConfig.spanUniqueClass -> "anotherInstance", ZipkinConfig.spanType -> "span4"),
        timestamp = new NanoTimestamp(NanoTimestamp.now.nanos + 300L)))
      val spanB2 = span.copy(trace = trace.copy(metadata = Map[String, String](HierarchyConfig.spanUniqueClass -> "anotherInstance", ZipkinConfig.spanType -> "span5"),
        timestamp = new NanoTimestamp(NanoTimestamp.now.nanos + 400L)))

      spanSubmitter ! new SpanBlock(mutable.Map[String, Span]("token3" -> spanA3, traceToken -> spanA1, "token2" -> spanA2, "token4" -> spanB1, "token5" -> spanB2), traceToken)

      expectMsg(Map[String, Span](traceToken -> spanA1, "token4" -> spanB1))
    }

    "give to each span correct parent" in new SpanTest {
      val spanSubmitter = TestActorRef(new SpanSubmitter(TraceSettings(system.settings.config)))

      val realParent = span.copy(trace = trace.copy(metadata = Map[String, String](HierarchyConfig.spanUniqueClass -> "parent", ZipkinConfig.spanType -> "span1")))
      val startParent = span.copy(trace = trace.copy(metadata = Map[String, String](HierarchyConfig.spanUniqueClass -> "parent", ZipkinConfig.spanType -> "span2"),
        timestamp = new NanoTimestamp(NanoTimestamp.now.nanos + 100L)), spanId = "anotherToken")
      val child = span.copy(trace = trace.copy(metadata = Map[String, String](ZipkinConfig.spanType -> "span3", ZipkinConfig.parentClass -> "parent"),
        timestamp = new NanoTimestamp(NanoTimestamp.now.nanos + 200L)), spanId = "child", parentSpanId = "anotherToken")

      spanSubmitter ! new SpanBlock(mutable.Map[String, Span](traceToken -> realParent, "token2" -> startParent, "token3" -> child), traceToken)

      expectMsg(Map[String, Span](traceToken -> realParent, "token3" -> child.copy(parentSpanId = realParent.spanId)))
    }

  }

}