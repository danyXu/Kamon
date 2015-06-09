package kamon.trace

import com.typesafe.config.ConfigFactory
import kamon.testkit.BaseKamonSpec
import kamon.util.NanoInterval

class SamplerSpec extends BaseKamonSpec("sampler-spec") {

  "the sampling strategy" should {
    "work as intended" when {
      "using all mode" in {
        val sampler = SampleAll

        sampler.shouldTrace should be(true)

        sampler.shouldReport(NanoInterval.default) should be(true)
      }

      "using random mode" in {
        val sampler = new RandomSampler(100)

        sampler.shouldTrace should be(true)
        sampler.shouldTrace should be(true)

        sampler.shouldReport(NanoInterval.default) should be(true)
      }

      "using ordered mode" in {
        var sampler = new OrderedSampler(1)

        sampler.shouldTrace should be(true)
        sampler.shouldTrace should be(true)
        sampler.shouldTrace should be(true)
        sampler.shouldTrace should be(true)
        sampler.shouldTrace should be(true)
        sampler.shouldTrace should be(true)

        sampler = new OrderedSampler(2)

        sampler.shouldTrace should be(false)
        sampler.shouldTrace should be(true)
        sampler.shouldTrace should be(false)
        sampler.shouldTrace should be(true)
        sampler.shouldTrace should be(false)
        sampler.shouldTrace should be(true)

        sampler.shouldReport(NanoInterval.default) should be(true)
      }

      "using threshold mode" in {
        val sampler = new ThresholdSampler(new NanoInterval(10000000L))

        sampler.shouldTrace should be(true)

        sampler.shouldReport(new NanoInterval(5000000L)) should be(false)
        sampler.shouldReport(new NanoInterval(10000000L)) should be(true)
        sampler.shouldReport(new NanoInterval(15000000L)) should be(true)
        sampler.shouldReport(new NanoInterval(0L)) should be(false)
      }

      "using clock mode" in {
        val sampler = new ClockSampler(new NanoInterval(10000000L))

        sampler.shouldTrace should be(true)
        sampler.shouldTrace should be(false)
        Thread.sleep(1L)
        sampler.shouldTrace should be(false)
        Thread.sleep(10L)
        sampler.shouldTrace should be(true)
        sampler.shouldTrace should be(false)

        sampler.shouldReport(NanoInterval.default) should be(true)
      }
    }
    "allow using a threshold sampler in addition to another sampler" in {
      val config = ConfigFactory.parseString(
        """
          |kamon {
          |  trace {
          |    level-of-detail = simple-trace
          |
          |    sampling = clock
          |    combine-threshold = true
          |    sampling-by-name = false
          |
          |    threshold-sampler {
          |      minimum-elapsed-time = 500 ms
          |    }
          |
          |    clock-sampler {
          |      pause = 100 ms
          |    }
          |
          |    token-name = ""
          |  }
          |}
        """.stripMargin)
      val settings = TraceSettings(config)
      settings.sampler("a").shouldTrace should be(true)
      settings.sampler("b").shouldTrace should be(false)
      Thread.sleep(100L)
      settings.sampler("c").shouldTrace should be(true)
      settings.sampler("d").shouldReport(new NanoInterval(100000000L)) should be(false)
      settings.sampler("e").shouldReport(new NanoInterval(500000000L)) should be(true)
    }
    "allow using an unique sampler per trace name" in {
      val config = ConfigFactory.parseString(
        """
          |kamon {
          |  trace {
          |    level-of-detail = simple-trace
          |
          |    sampling = clock
          |    combine-threshold = false
          |    sampling-by-name = true
          |
          |    clock-sampler {
          |      pause = 100 ms
          |    }
          |
          |    token-name = ""
          |  }
          |}
        """.stripMargin)
      val settings = TraceSettings(config)
      settings.sampler("a").shouldTrace should be(true)
      settings.sampler("a").shouldTrace should be(false)
      settings.sampler("b").shouldTrace should be(true)
      settings.sampler("c").shouldTrace should be(true)
      settings.sampler("b").shouldTrace should be(false)
      Thread.sleep(100L)
      settings.sampler("a").shouldTrace should be(true)
      settings.sampler("a").shouldTrace should be(false)
      settings.sampler("b").shouldTrace should be(true)
      settings.sampler("b").shouldTrace should be(false)
      settings.sampler("c").shouldTrace should be(true)
      settings.sampler("c").shouldTrace should be(false)
    }
    "allow combining "

  }

}
