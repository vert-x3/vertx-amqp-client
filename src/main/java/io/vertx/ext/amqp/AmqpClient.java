package io.vertx.ext.amqp;


import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.amqp.impl.AmqpClientImpl;

import java.util.Objects;

/**
 * AMQP Client entry point.
 * Use this interface to create an instance of {@link AmqpClient} and connect to a broker and server.
 */
@VertxGen
public interface AmqpClient {

  /**
   * Creates a new instance of {@link AmqpClient} using an internal Vert.x instance (with default configuration) and
   * the given AMQP client configuration. Note that the created Vert.x instance will be closed when the client is
   * closed.
   *
   * @param options the AMQP client options, may be {@code null} falling back to the default configuration
   * @return the created instances.
   */
  static AmqpClient create(AmqpClientOptions options) {
    return new AmqpClientImpl(Vertx.vertx(), options, true);
  }

  /**
   * Creates a new instance of {@link AmqpClient} with the given Vert.x instance and the given options.
   *
   * @param vertx   the vert.x instance, must not be {@code null}
   * @param options the AMQP options, may be @{code null} falling back to the default configuration
   * @return the AMQP client instance
   */
  static AmqpClient create(Vertx vertx, AmqpClientOptions options) {
    return new AmqpClientImpl(Objects.requireNonNull(vertx), options, false);
  }

  /**
   * Connects to the AMQP broker or router. The location is specified in the {@link AmqpClientOptions} as well as the
   * potential credential required.
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

}
