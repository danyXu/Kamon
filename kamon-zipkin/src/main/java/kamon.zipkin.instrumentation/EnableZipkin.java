package kamon.zipkin.instrumentation;

import java.lang.annotation.*;

/**
 * A marker annotation to enable the Kamon Zipkin instrumentation.
 *
 * The AspectJ Weaver will scan all the declared methods of the annotated class that are annotated with
 * some Kamon annotations, then create and register the corresponding instruments instances and finally weave
 * its aspects around these methods, so that at runtime, these instruments instances get called according
 * to the Kamon annotations specification.
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
public @interface EnableZipkin {}