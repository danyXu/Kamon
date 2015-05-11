package kamon.zipkin.actor

import akka.actor.{ ActorLogging, Actor }
import com.github.levkhomich.akka.tracing.TracingExtensionImpl
import kamon.trace.HierarchyConfig
import kamon.util.NanoTimestamp
import kamon.zipkin.ZipkinConfig
import kamon.zipkin.models.{ Span, SpanBlock }

import scala.collection.mutable

/*
 * This actor is used to submit spans to akka-tracing @ https://github.com/levkhomich/akka-tracing
 * It could be "improved" to directly submit spans to Zipkin without having to use a library
 */
class SpanSubmitter(tracingExt: TracingExtensionImpl) extends Actor with ActorLogging {

  def receive = {
    case spanBlock: SpanBlock ⇒
      val spans = spanBlock.spans
      if (spanBlock.remote || spans.contains(spanBlock.rootToken)) tracingExt.submitSpans(buildHierarchy(spans).map(_._2.simpleSpan))
  }

  private def buildHierarchy(spans: mutable.Map[String, Span]): mutable.Map[String, Span] = {
    val traceInstance = mutable.Map.empty[String, String]

    spans.toSeq.sortBy(_._2.trace.timestamp.nanos).foreach {
      case (token, span) ⇒
        val trace = span.trace
        val instance = trace.metadata.getOrElse(HierarchyConfig.spanUniqueClass, "")

        traceInstance.get(instance) match {
          case Some(instanceToken) ⇒ spans.get(instanceToken) match {
            case Some(realSpan) ⇒
              realSpan.addToRecords(
                ZipkinConfig.segmentBegin + trace.metadata.getOrElse(ZipkinConfig.spanType, "unknown"),
                new NanoTimestamp(trace.timestamp.nanos))
              realSpan.addToRecords(
                ZipkinConfig.segmentEnd + trace.metadata.getOrElse(ZipkinConfig.spanType, "unknown"),
                new NanoTimestamp(trace.timestamp.nanos + trace.elapsedTime.nanos))
              realSpan.addSegments(trace.segments)

              spans += (instanceToken -> realSpan)
              spans -= token
            case None ⇒
          }
          case None ⇒ traceInstance += (instance -> token)
        }
    }

    spans.toSeq.sortBy(_._2.trace.timestamp.nanos).foreach {
      case (token, span) ⇒
        span.trace.metadata.get(ZipkinConfig.parentClass) match {
          case Some(parentInstance) ⇒ traceInstance.get(parentInstance) match {
            case Some(parentToken) ⇒ spans += (token -> span.copy(parentSpanId = parentToken))
            case None              ⇒
          }
          case None ⇒
        }
    }

    spans
  }

}