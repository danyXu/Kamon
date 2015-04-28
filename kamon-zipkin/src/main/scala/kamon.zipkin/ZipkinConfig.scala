package kamon.zipkin

object ZipkinConfig {
  val internalPrefix = "internal."

  val spanClass = internalPrefix + "class"
  val spanType = internalPrefix + "type"
  val spanUniqueClass = internalPrefix + "uniqueClass"

  val segmentBegin = "BEGIN> "
  val segmentEnd = "END> "

  val recordMinDuration = 100
}
