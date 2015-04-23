package kamon.zipkin.instrumentation

import kamon.Kamon
import kamon.testkit.BaseKamonSpec
import kamon.trace.{ TraceInfo, Tracer }

class EnableZipkinInstrumentationSpec extends BaseKamonSpec("enable-zipkin-instrumentation-spec") {

  "The enable instrumentation of zipkin" should {

    "log method executions in classes with EnableZipkin annotation" in {
      Kamon.tracer.subscribe(testActor)

      Tracer.withContext(newContext("testKamonZipkin")) {
        val hello = new TestAnnotation()
        hello.helloZipkin()
        Tracer.currentContext.finish()
      }

      val traceInfo = expectMsgType[TraceInfo]
      Kamon.tracer.unsubscribe(testActor)

      traceInfo.segments.size should be(1)
      traceInfo.segments.find(_.name == "helloZipkin()") should be('defined)
    }

  }

}

@EnableZipkin
class TestAnnotation { def helloZipkin() = "hello" }
