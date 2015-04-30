package kamon.zipkin.models

import java.nio.ByteBuffer

import com.github.levkhomich.akka.tracing.thrift
import kamon.trace.{ SegmentInfo, TraceInfo }
import kamon.util.{ NanoInterval, NanoTimestamp }
import kamon.zipkin.ZipkinConfig

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class Span(trace: TraceInfo, traceId: Long, spanId: Long, name: String, start: Long, duration: Long,
    annotations: mutable.Map[String, String], parentSpanId: Long = 0, endpoint: thrift.Endpoint = Endpoint.createApplicationEndpoint("zipkin"),
    isClient: Boolean = false, records: mutable.Map[String, NanoTimestamp] = mutable.Map[String, NanoTimestamp]()) {

  val otherSegments = ListBuffer.empty[SegmentInfo]

  def simpleSpan = {
    var maxTimestamp = start + duration
    var fullName = name

    val span = new thrift.Span()
    span.set_trace_id(traceId)
    span.set_id(spanId)
    span.set_parent_id(parentSpanId)

    annotations.foreach { case (k, v) ⇒ span.add_to_binary_annotations(stringAnnotation(k, v)) }
    trace.segments.filter(s ⇒ filterSegments(s)).foreach {
      segment ⇒ span.add_to_annotations(timestampAnnotation(segment.name, segment.timestamp, segment.elapsedTime))
    }
    otherSegments.filter(s ⇒ filterSegments(s)).foreach {
      segment ⇒ span.add_to_annotations(timestampAnnotation(segment.name, segment.timestamp, segment.elapsedTime))
    }
    records.foreach {
      case (k, v) ⇒
        if (TimestampConverter.timestampToMicros(v) > maxTimestamp) maxTimestamp = TimestampConverter.timestampToMicros(v)
        span.add_to_annotations(timestampAnnotation(k, v, NanoInterval.default))
        if (k.startsWith(ZipkinConfig.segmentBegin)) fullName += " - " + k.stripPrefix(ZipkinConfig.segmentBegin)
    }

    span.set_name(fullName)

    val sa = new thrift.Annotation(start, if (isClient) thrift.zipkinConstants.CLIENT_SEND else thrift.zipkinConstants.SERVER_RECV)
    sa.set_host(endpoint)
    val ea = new thrift.Annotation(maxTimestamp, if (isClient) thrift.zipkinConstants.CLIENT_RECV else thrift.zipkinConstants.SERVER_SEND)
    ea.set_host(sa.get_host())

    val begin = new thrift.Annotation(start, ZipkinConfig.segmentBegin + name)
    val end = new thrift.Annotation(start + duration, ZipkinConfig.segmentEnd + name)

    span.add_to_annotations(sa)
    span.add_to_annotations(begin)
    span.add_to_annotations(end)
    span.add_to_annotations(ea)

    span
  }

  def addToAnnotations(key: String, value: String) = annotations += (key -> value)
  def addToRecords(key: String, value: NanoTimestamp) = records += (key -> value)
  def addSegments(newSegments: List[SegmentInfo]) = otherSegments ++= newSegments

  private def stringAnnotation(key: String, value: String) = {
    val a = new thrift.BinaryAnnotation()
    a.set_annotation_type(thrift.AnnotationType.STRING)
    a.set_key(key)
    a.set_value(ByteBuffer.wrap(value.getBytes))
    a
  }

  private def timestampAnnotation(key: String, value: NanoTimestamp, elapsedTime: NanoInterval) = {
    val a = new thrift.Annotation()
    a.set_value(key)
    a.set_timestamp(TimestampConverter.timestampToMicros(value))
    val duration = TimestampConverter.durationToMicros(elapsedTime).toInt
    if (duration > ZipkinConfig.recordMinDuration) a.set_duration(duration)
    a
  }

  private def filterSegments(s: SegmentInfo): Boolean = {
    val duration = TimestampConverter.durationToMicros(s.elapsedTime).toInt
    duration == 0 || duration > ZipkinConfig.recordMinDuration
  }
}

object TimestampConverter {
  def timestampToMicros(nano: NanoTimestamp) = nano.nanos / 1000
  def durationToMicros(nano: NanoInterval) = nano.nanos / 1000
}

case class SpanBlock(spans: Iterable[thrift.Span])
