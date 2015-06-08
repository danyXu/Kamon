package kamon.zipkin.instrumentation

import kamon.Kamon
import kamon.trace.{ LevelOfDetail, Tracer }
import kamon.util.SameThreadExecutionContext
import kamon.zipkin.Zipkin
import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.annotation.{ Aspect, Pointcut, Around }
import org.aspectj.lang.reflect.{ CodeSignature, MethodSignature }

import scala.collection.mutable.ListBuffer

@Aspect
abstract class EnableZipkinInstrumentation {

  var checkToString = false

  @Pointcut("execution(* *(..)) && !execution(* akka..*(..)) && !execution(* scala..*(..)) && within(@kamon.zipkin.instrumentation.EnableZipkin *)")
  def enableZipkinPointcut() = {}

  /**
   * This pointcut can be overridden in the aop.xml using a concrete-aspect to enable zipkin record on
   * compiled files methods which can't be edited. So basically, you can use this "hack" to add the
   * "@EnableZipkin" annotation to any dependency you want to trace.
   *
   * For example, if you want to record when an object is serialized using an external library like
   * "play.api.libs.json", you should add into the div "<aspectj>" of your aop.xml file :
   *
   * <aspectj>
   *   <aspects>
   *     <concrete-aspect name="kamon.zipkin.instrumentation.CustomZipkinInstrumentation" extends="kamon.zipkin.instrumentation.EnableZipkinInstrumentation">
   *       <pointcut name="optionalZipkinPointcut" expression="execution(* *.stringify(..)) || execution(* *.toJson(..))" />
   *     </concrete-aspect>
   *   </aspects>
   * <aspectj>
   */
  @Pointcut()
  def optionalZipkinPointcut() = {}

  @Around(value = "enableZipkinPointcut() || optionalZipkinPointcut()")
  def aroundMethodsEnabled(pjp: ProceedingJoinPoint): Any =
    if (Tracer.currentContext.nonEmpty && Tracer.currentContext.levelOfDetail != LevelOfDetail.MetricsOnly && !checkToString) {

      val currentContext = Tracer.currentContext
      val recordParams = Kamon(Zipkin).config.recordParamsValue

      val annotations = ListBuffer.empty[(String, String)]

      val args = pjp.getSignature.asInstanceOf[CodeSignature].getParameterNames.zip(pjp.getArgs).map {
        case (argName, argVal) if argVal == null ⇒
          argName + ": null"
        case (argName, argVal) ⇒
          if (recordParams) {
            val argNameRdm = argName + "[" + java.util.UUID.randomUUID.toString + "]"
            checkToString = true
            val arg = argVal.toString
            annotations += ((argNameRdm, arg))
            checkToString = false
            argNameRdm
          } else {
            argName
          } + ": " + argVal.getClass.getSimpleName
      }

      val txt = new StringBuilder("- ") * currentContext.segmentsCount + pjp.getSignature.getName + "(" + args.mkString(", ") + ")"

      pjp.getSignature.asInstanceOf[MethodSignature].getReturnType.toString match {
        case "interface scala.concurrent.Future" ⇒
          val segment = currentContext.startSegment(txt, "zipkin", "kamon")
          val result = pjp.proceed().asInstanceOf[scala.concurrent.Future[Any]]
          annotations.foreach { case (name, value) ⇒ segment.addMetadata(name, value) }
          result.onComplete(_ ⇒ segment.finish())(SameThreadExecutionContext)
          result
        case _ ⇒
          val segment = currentContext.startSegment(txt, "zipkin", "kamon")
          val result = pjp.proceed()
          annotations.foreach { case (name, value) ⇒ segment.addMetadata(name, value) }
          segment.finish()
          result
      }

    } else pjp.proceed()

}