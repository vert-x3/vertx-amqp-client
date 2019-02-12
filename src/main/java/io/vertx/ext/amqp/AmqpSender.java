package io.vertx.ext.amqp;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

// TODO Implement write stream.
@VertxGen
public interface AmqpSender {

  @Fluent
  AmqpSender send(AmqpMessage message);

  @Fluent
  AmqpSender send(String address, AmqpMessage message);

  @Fluent
  AmqpSender sendWithAck(AmqpMessage message, Handler<AsyncResult<Void>> acknowledgementHandler);

  @Fluent
  AmqpSender sendWithAck(String address, AmqpMessage message, Handler<AsyncResult<Void>> acknowledgementHandler);

  void close(Handler<AsyncResult<Void>> handler);

  String address();
}
