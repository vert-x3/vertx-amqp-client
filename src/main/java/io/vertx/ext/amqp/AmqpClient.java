package io.vertx.ext.amqp;


import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.amqp.impl.AmqpClientImpl;

import java.util.Objects;

@VertxGen
public interface AmqpClient {

  static AmqpClient create() {
    return new AmqpClientImpl(Vertx.vertx(), new AmqpClientOptions());
  }

  static AmqpClient create(AmqpClientOptions options) {
    return new AmqpClientImpl(Vertx.vertx(), options);
  }

  /**
   * Connect to the AMQP borker or router, without credentials.
   *
   * @param connectionHandler handler that will process the result, giving either the connection or failure cause.
   */
  @Fluent
  AmqpClient connect(Handler<AsyncResult<AmqpConnection>> connectionHandler);

  /**
   * Creates a default instance of {@link AmqpClient}
   *
   * @param vertx   the vert.x instance, must not be {@code null}
   * @param options the AMQP options
   * @return the AMQP client instance
   */
  static AmqpClient create(Vertx vertx, AmqpClientOptions options) {
    return new AmqpClientImpl(Objects.requireNonNull(vertx), options);
  }
}
