package kamon.zipkin.instrumentation;

import java.lang.annotation.*;

/**
 + * A marker annotation to enable the Kamon Zipkin instrumentation.
 + */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface EnableZipkin {}