package kamon.zipkin.actor

import java.net.InetAddress
import java.nio.ByteBuffer

import akka.actor.{ ActorRef, Actor }
import com.github.levkhomich.akka.tracing.thrift
import kamon.akka.remote.RemoteConfig
import kamon.trace.{ SegmentInfo, TraceInfo }
import kamon.util.{ NanoInterval, NanoTimestamp }
import kamon.zipkin.ZipkinConfig
import tv.teads.gaia.logging.LazyLogging

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class ZipkinActor(spansSubmitter: ActorRef) extends Actor with LazyLogging {

  private val marker = "EndpointWriter"
  var parentActor = false

  /*
   * Associate to each token corresponding span
   * (rootToken -> (token -> span))
   */
  val traceSpan = TrieMap.empty[String, TrieMap[String, Span]]
  /*
   * Associate to each instance corresponding token
   * (rootToken -> (instance -> token))
   */
  val traceInstance = TrieMap.empty[String, TrieMap[String, String]]
  /*
   * Associate to each token corresponding real parentToken
   * (rootToken -> (token -> realParentToken))
   */
  val traceParent = TrieMap.empty[String, TrieMap[String, String]]

  def receive = {
    case trace: TraceInfo ⇒
      val rootToken = trace.metadata.getOrElse(RemoteConfig.rootToken, "")
      val parentToken = trace.metadata.getOrElse(RemoteConfig.parentToken, "")

      // Instantiate TrieMap for this rootToken if it wasn't done before
      if (!traceInstance.contains(rootToken)) traceInstance += (rootToken -> TrieMap[String, String]())
      if (!traceParent.contains(rootToken)) traceParent += (rootToken -> TrieMap[String, String]())

      // Create span of this TraceInfo and add it to the TrieMap
      val span = traceInfoToSpans(trace)
      if (traceSpan.contains(rootToken)) traceSpan += (rootToken -> (traceSpan(rootToken) += (trace.token -> span)))
      else traceSpan += (rootToken -> TrieMap(trace.token -> span))

      checkUnicity(trace, rootToken)

      // Dirty param to know if this actor is the local which started to work on the request
      if (!parentActor && parentToken == rootToken) parentActor = true
      // Send spans periodically to Zipkin
      if (trace.token == rootToken || (!parentActor && trace.metadata.getOrElse(ZipkinConfig.spanClass, "") == marker)) {
        spansSubmitter ! new SpanBlock(traceSpan(rootToken).map(_._2.simpleSpan))

        traceSpan -= rootToken
        traceInstance -= rootToken
        traceParent -= rootToken
      }
  }

  // Check if this instance is part of an already existing span
  private def checkUnicity(trace: TraceInfo, rootToken: String) = {
    val instance = trace.metadata.getOrElse(ZipkinConfig.spanUniqueClass, "")
    if (traceInstance(rootToken).contains(instance) && traceSpan(rootToken).contains(traceInstance(rootToken)(instance))) {
      val realParentToken = traceInstance(rootToken)(instance)
      traceSpan(rootToken)(realParentToken).addToRecords(
        ZipkinConfig.segmentBegin + trace.metadata.getOrElse(ZipkinConfig.spanType, "unknown"),
        new NanoTimestamp(trace.timestamp.nanos))
      traceSpan(rootToken)(realParentToken).addToRecords(
        ZipkinConfig.segmentEnd + trace.metadata.getOrElse(ZipkinConfig.spanType, "unknown"),
        new NanoTimestamp(trace.timestamp.nanos + trace.elapsedTime.nanos))

      traceSpan(rootToken)(realParentToken).addSegments(trace.segments)

      traceSpan(rootToken) -= trace.token
      traceParent(rootToken) += (trace.token -> realParentToken)
    } else {
      traceInstance(rootToken) += (instance -> trace.token)
    }
  }

  private def traceInfoToSpans(trace: TraceInfo): Span = {
    val rootToken = trace.metadata.getOrElse(RemoteConfig.rootToken, "")
    var parentToken = trace.metadata.getOrElse(RemoteConfig.parentToken, "")
    if (traceParent(rootToken).contains(parentToken)) parentToken = traceParent(rootToken)(parentToken)
    val token = trace.token

    val traceId = longHash(rootToken)
    val rootSpanId = longHash(token)
    val parentSpanId = longHash(parentToken)

    val cleanMetaData = mutable.Map(trace.metadata.filterKeys(k ⇒ !k.startsWith(ZipkinConfig.internalPrefix)).toSeq: _*)
    val endpoint = Endpoint.createApplicationEndpoint(trace.metadata.getOrElse(ZipkinConfig.spanClass, "Request"))

    Span(trace, traceId, rootSpanId, trace.name, TimestampConverter.timestampToMicros(trace.timestamp),
      TimestampConverter.durationToMicros(trace.elapsedTime), cleanMetaData, parentSpanId, endpoint)
  }

  private def longHash(string: String): Long = {
    var h = 1125899906842597L
    val len = string.length
    for (i ← 0 until len) h = 31 * h + string.charAt(i)
    h
  }
}

object Endpoint {
  def createApplicationEndpoint(service: String) =
    createEndpoint(service, "localhost", 9410)

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

object TimestampConverter {
  def timestampToMicros(nano: NanoTimestamp) = nano.nanos / 1000
  def durationToMicros(nano: NanoInterval) = nano.nanos / 1000
}

case class Span(trace: TraceInfo, traceId: Long, spanId: Long, name: String, start: Long, duration: Long,
    annotations: mutable.Map[String, String], parentSpanId: Long = 0, endpoint: thrift.Endpoint = Endpoint.createApplicationEndpoint("zipkin"),
    isClient: Boolean = false, records: mutable.Map[String, NanoTimestamp] = mutable.Map[String, NanoTimestamp]()) extends LazyLogging {

  val otherSegments = ListBuffer.empty[SegmentInfo]

  def simpleSpan = {
    var maxTimestamp = start + duration
    var fullName = name

    val span = new thrift.Span()
    span.set_trace_id(traceId)
    span.set_id(spanId)
    span.set_parent_id(parentSpanId)

    annotations.foreach { case (k, v) ⇒ span.add_to_binary_annotations(stringAnnotation(k, v)) }
    trace.segments.foreach { segment ⇒ span.add_to_annotations(timestampAnnotation(segment.name, segment.timestamp, segment.elapsedTime)) }
    otherSegments.foreach { segment ⇒ span.add_to_annotations(timestampAnnotation(segment.name, segment.timestamp, segment.elapsedTime)) }
    records.foreach {
      case (k, v) ⇒
        if (TimestampConverter.timestampToMicros(v) > maxTimestamp) maxTimestamp = TimestampConverter.timestampToMicros(v)
        span.add_to_annotations(timestampAnnotation(k, v, NanoInterval.default))
        if (k.startsWith(ZipkinConfig.segmentBegin)) fullName += " - " + k.stripPrefix(ZipkinConfig.segmentBegin)
    }

    span.set_name(fullName)

    val sa = new thrift.Annotation()
    sa.set_timestamp(start)
    sa.set_value(if (isClient) thrift.zipkinConstants.CLIENT_SEND else thrift.zipkinConstants.SERVER_RECV)
    sa.set_host(endpoint)
    val ea = new thrift.Annotation()
    ea.set_timestamp(maxTimestamp)
    ea.set_value(if (isClient) thrift.zipkinConstants.CLIENT_RECV else thrift.zipkinConstants.SERVER_SEND)
    ea.set_host(sa.get_host())

    val begin = new thrift.Annotation()
    begin.set_timestamp(start)
    begin.set_value(ZipkinConfig.segmentBegin + name)
    val end = new thrift.Annotation()
    end.set_timestamp(start + duration)
    end.set_value(ZipkinConfig.segmentEnd + name)

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
    if (elapsedTime != NanoInterval.default) a.set_duration(TimestampConverter.durationToMicros(elapsedTime).toInt)
    a
  }
}

case class SpanBlock(spans: Iterable[thrift.Span])