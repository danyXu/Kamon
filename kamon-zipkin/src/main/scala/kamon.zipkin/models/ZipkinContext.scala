package kamon.trace

import akka.event.LoggingAdapter
import kamon.util.RelativeNanoTimestamp

class ZipkinContext(traceName: String, token: String, izOpen: Boolean, levelOfDetail: LevelOfDetail, isLocal: Boolean,
  startTimeztamp: RelativeNanoTimestamp, log: LoggingAdapter, traceInfoSink: TracingContext ⇒ Unit)
    extends TracingContext(traceName, token, izOpen, levelOfDetail, isLocal, startTimeztamp, log, traceInfoSink) {

}
