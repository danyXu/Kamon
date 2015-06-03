include "scribe.thrift"

namespace java kamon.zipkin.thrift

const string CLIENT_SEND = "cs"
const string CLIENT_RECV = "cr"
const string SERVER_SEND = "ss"
const string SERVER_RECV = "sr"

struct Endpoint {
  1: i32 ipv4,
  2: i16 port,
  3: string service_name
}

struct Annotation {
  1: i64 timestamp,
  2: string value,
  3: optional Endpoint host,
  4: optional i32 duration
}

enum AnnotationType {
  BOOL,
  BYTES,
  I16,
  I32,
  I64,
  DOUBLE,
  STRING
}

struct BinaryAnnotation {
  1: string key,
  2: binary value,
  3: AnnotationType annotation_type,
  4: optional Endpoint host
}

struct Span {
  1: i64 trace_id,
  3: string name,
  4: i64 id,
  5: optional i64 parent_id,
  6: list<Annotation> annotations,
  8: list<BinaryAnnotation> binary_annotations,
  9: optional bool debug = 0
}

