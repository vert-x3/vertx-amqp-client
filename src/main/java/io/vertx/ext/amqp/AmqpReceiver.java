package io.vertx.ext.amqp;

import io.vertx.codegen.annotations.CacheReturn;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

@VertxGen
public interface AmqpReceiver {

  @CacheReturn
  String address();

  @Fluent
  AmqpReceiver close();

  @Fluent
  AmqpReceiver close(Handler<AsyncResult<Void>> completionHandler);

}
