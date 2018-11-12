package io.vertx.ext.amqp;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.streams.WriteStream;

@VertxGen
public interface AmqpSender extends WriteStream<AmqpMessage> {

  @Fluent
  AmqpSender send(AmqpMessage message);

  @Fluent
  AmqpSender send(String address, AmqpMessage message);

  @Fluent
  AmqpSender sendWithAck(AmqpMessage message, Handler<AsyncResult<Void>> acknowledgementHandler);

  @Fluent
  AmqpSender sendWithAck(String address, AmqpMessage message, Handler<AsyncResult<Void>> acknowledgementHandler);

  @Override
  @Fluent
  AmqpSender exceptionHandler(Handler<Throwable> handler);

  @Override
  @Fluent
  AmqpSender write(AmqpMessage message);

  @Override
  void end();

  @Override
  @Fluent
  AmqpSender setWriteQueueMaxSize(int i);

  @Override
  boolean writeQueueFull();

  @Override
  @Fluent
  AmqpSender drainHandler(@Nullable Handler<Void> handler);
}
