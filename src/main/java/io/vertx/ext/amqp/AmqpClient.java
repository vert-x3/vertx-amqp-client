package io.vertx.ext.amqp;


import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.amqp.impl.AmqpClientImpl;

import java.util.Objects;

@VertxGen
public interface AmqpClient {

  static AmqpClient create() {
    return new AmqpClientImpl(Vertx.vertx(), new AmqpClientOptions(), true);
  }

  static AmqpClient create(AmqpClientOptions options) {
    return new AmqpClientImpl(Vertx.vertx(), options, true);
  }

  /**
   * Connect to the AMQP borker or router, without credentials.
   *
   * @param connectionHandler handler that will process the result, giving either the connection or failure cause. Must
   *                          not be {@code null}.
   */
  @Fluent
  AmqpClient connect(Handler<AsyncResult<AmqpConnection>> connectionHandler);

  /**
   * Closes the client.
   * The client must always be closed once not needed anymore.
   *
   * @param closeHandler the close handler notified when the operation completes. It can be {@code null}.
   */
  void close(@Nullable Handler<AsyncResult<Void>> closeHandler);


  /**
   * Creates a default instance of {@link AmqpClient}
   *
   * @param vertx   the vert.x instance, must not be {@code null}
   * @param options the AMQP options, may be @{code null}.
   * @return the AMQP client instance
   */
  static AmqpClient create(Vertx vertx, AmqpClientOptions options) {
    return new AmqpClientImpl(Objects.requireNonNull(vertx), options, false);
  }
}
