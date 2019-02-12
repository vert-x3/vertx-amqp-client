package io.vertx.ext.amqp;

import io.vertx.codegen.annotations.CacheReturn;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.streams.ReadStream;

@VertxGen
public interface AmqpReceiver extends ReadStream<AmqpMessage> {

  @CacheReturn
  String address();

  void close(Handler<AsyncResult<Void>> handler);

}
