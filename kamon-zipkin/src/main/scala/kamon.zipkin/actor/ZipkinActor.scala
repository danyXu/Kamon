package kamon.zipkin.actor

import akka.actor._
import kamon.trace.{ HierarchyConfig, TraceInfo }
import kamon.util.NanoTimestamp
import kamon.zipkin.ZipkinConfig
import kamon.zipkin.models._

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

class ZipkinActorSupervisor(spansSubmitter: ActorRef, config: ZipkinConfig) extends Actor with ActorLogging {

  val tokenActors = TrieMap.empty[String, ActorRef]

  def receive: Receive = {
    case trace: TraceInfo ⇒
      trace.metadata.get(HierarchyConfig.rootToken) match {
        case Some(rootToken) ⇒
          tokenActors.get(rootToken) match {
            case Some(tokenActor) ⇒ tokenActor ! trace
            case None ⇒
              val tokenActor = context.actorOf(Props(new ZipkinActor(spansSubmitter, config, rootToken,
                trace.metadata.contains(ZipkinConfig.remote))))
              tokenActors += (rootToken -> tokenActor)
              tokenActor ! trace
          }
        case None ⇒ log.error(s"TraceInfo doesn't have a ${HierarchyConfig.rootToken}")
      }
  }

}

class ZipkinActor(spansSubmitter: ActorRef, config: ZipkinConfig, rootToken: String, remote: Boolean) extends Actor {

  /*
   * Associate to each token corresponding span
   * token -> span
   */
  val traceSpan = TrieMap.empty[String, Span]
  /*
   * Associate to each instance corresponding token
   * instance -> token
   */
  val traceInstance = TrieMap.empty[String, String]
  /*
   * Associate to each token corresponding real parentToken
   * token -> realParentToken
   */
  val traceParent = TrieMap.empty[String, String]

  def receive: Receive = {
    case trace: TraceInfo ⇒
      // Create span of this TraceInfo and add it to the TrieMap
      traceSpan += (trace.token -> traceInfoToSpans(trace))

      checkUnicity(trace)

      // Send spans to Zipkin
      if (trace.token == rootToken || (remote && trace.metadata.getOrElse(ZipkinConfig.spanClass, "") == ZipkinConfig.endpointMarker)) {
        spansSubmitter ! new SpanBlock(traceSpan.map(_._2.simpleSpan))
        context.stop(self)
      }
  }

  override def postStop() = {
    traceSpan.clear()
    traceInstance.clear()
    traceParent.clear()
  }

  // Check if this instance is part of an already existing span
  private def checkUnicity(trace: TraceInfo) = {
    val instance = trace.metadata.getOrElse(ZipkinConfig.spanUniqueClass, "")

    traceInstance.get(instance) match {
      case Some(realParentToken) ⇒ traceSpan.get(realParentToken) match {
        case Some(span) ⇒
          span.addToRecords(
            ZipkinConfig.segmentBegin + trace.metadata.getOrElse(ZipkinConfig.spanType, "unknown"),
            new NanoTimestamp(trace.timestamp.nanos))
          span.addToRecords(
            ZipkinConfig.segmentEnd + trace.metadata.getOrElse(ZipkinConfig.spanType, "unknown"),
            new NanoTimestamp(trace.timestamp.nanos + trace.elapsedTime.nanos))
          span.addSegments(trace.segments)

          traceSpan -= trace.token
          traceParent += (trace.token -> realParentToken)
        case None ⇒
          traceInstance += (instance -> trace.token)
      }
      case None ⇒ traceInstance += (instance -> trace.token)
    }
  }

  private def traceInfoToSpans(trace: TraceInfo): Span = {
    val parentMeta = trace.metadata.getOrElse(HierarchyConfig.parentToken, "")
    val parentToken = traceParent.getOrElse(parentMeta, parentMeta)
    val token = trace.token

    val traceId = longHash(rootToken)
    val rootSpanId = longHash(token)
    val parentSpanId = longHash(parentToken)

    val cleanMetaData = mutable.Map(trace.metadata
      .filterKeys(k ⇒ !k.startsWith(ZipkinConfig.internalPrefix) && k != HierarchyConfig.rootToken && k != HierarchyConfig.parentToken).toSeq: _*)
    val endpoint = Endpoint.createEndpoint(trace.metadata.getOrElse(ZipkinConfig.spanClass, "Request"), config.service.host, config.service.port)

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