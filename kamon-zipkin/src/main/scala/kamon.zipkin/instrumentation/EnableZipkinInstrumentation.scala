package kamon.zipkin.instrumentation

import kamon.trace.{ LevelOfDetail, Tracer }
import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.annotation.{ Aspect, Pointcut, Around }

@Aspect
abstract class EnableZipkinInstrumentation {

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
    if (!Tracer.currentContext.isEmpty && Tracer.currentContext.levelOfDetail != LevelOfDetail.MetricsOnly) {
      /*
      val args = pjp.getArgs.foldLeft(Map.empty[String, String]) {
        case (map, element) if element == null => map + ("null" -> "null")
        case (map, element)                    => map + (element.getClass.getSimpleName -> element.toString)
      }
      */

      val args = pjp.getArgs.foldLeft(List.empty[String]) {
        case (list, arg) if arg == null ⇒ list :+ "null"
        case (list, arg)                ⇒ list :+ arg.getClass.getSimpleName
      }
      val txt = pjp.getSignature.getName + "(" + args.mkString(", ") + ")"

      Tracer.currentContext.withNewSegment(txt, "zipkin", "kamon") { pjp.proceed() }
    } else pjp.proceed()

}