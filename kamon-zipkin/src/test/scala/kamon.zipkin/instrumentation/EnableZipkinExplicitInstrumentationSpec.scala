package kamon.zipkin.instrumentation

import kamon.Kamon
import kamon.testkit.BaseKamonSpec
import kamon.trace.{ TraceInfo, Tracer }

class EnableZipkinExplicitInstrumentationSpec extends BaseKamonSpec("enable-zipkin-explicit-instrumentation-spec") {

  "the explicit instrumentation of zipkin" should {

    "log execution of methods explicitly added to aop.xml file" in {
      Kamon.tracer.subscribe(testActor)

      Tracer.withContext(newContext("testKamonZipkin")) {
        val hello = new TestExplicitAnnotation()
        hello.helloZipkinExplicit()
        Tracer.currentContext.finish()
      }

      val traceInfo = expectMsgType[TraceInfo]
      Kamon.tracer.unsubscribe(testActor)

      traceInfo.segments.size should be(1)
      traceInfo.segments.find(_.name == "helloZipkinExplicit()") should be('defined)
    }

  }

}

class TestExplicitAnnotation { def helloZipkinExplicit() = "hello explicit" }