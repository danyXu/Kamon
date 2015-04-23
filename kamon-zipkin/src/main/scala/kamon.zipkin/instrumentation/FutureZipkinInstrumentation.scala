package kamon.zipkin.instrumentation

import kamon.trace.{ LevelOfDetail, Tracer }
import org.aspectj.lang.JoinPoint
import org.aspectj.lang.annotation.{ Before, Aspect, Pointcut }

import scala.concurrent.{ Promise, Future }

/*
 * CURRENTLY USELESS
 */

@Aspect
class FutureZipkinInstrumentation {

  @Pointcut(value = "execution(scala.concurrent.Future+.new(..)) && this(future)")
  def futurePointcut(future: Promise[Any]) = {}

  @Pointcut(value = "execution(* scala.concurrent.Promise+.tryComplete(..)) && this(value)")
  def futureTryCompletePointcut(value: Future[Any]) = {}

  @Pointcut(value = "execution(* scala.concurrent.Future+.ready(..)) && this(value)")
  def futureReadyPointcut(value: Future[Any]) = {}

  @Before(value = "futurePointcut(promise)", argNames = "jp, promise")
  def futureBefore(jp: JoinPoint, promise: Promise[Any]): Unit = {
    if (Tracer.currentContext.nonEmpty && Tracer.currentContext.levelOfDetail != LevelOfDetail.MetricsOnly)
      Tracer.currentContext.startSegment(s"${promise.future.value} - START", "zipkin", "kamon").finish()
  }

  @Before(value = "futureReadyPointcut(value) || futureTryCompletePointcut(value)", argNames = "jp, value")
  def futureReadyBefore(jp: JoinPoint, value: Future[Any]): Unit = {
    if (Tracer.currentContext.nonEmpty && Tracer.currentContext.levelOfDetail != LevelOfDetail.MetricsOnly) {
      Tracer.currentContext.startSegment(value.value + " - END", "zipkin", "kamon").finish()
    }
  }

}
