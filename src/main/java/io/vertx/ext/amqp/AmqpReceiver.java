package io.vertx.ext.amqp;

import io.vertx.codegen.annotations.CacheReturn;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.streams.ReadStream;

/**
 * Interface used to consume AMQP message as a stream of message.
 * Back pressure is implemented using AMQP credits.
 */
@VertxGen
public interface AmqpReceiver extends ReadStream<AmqpMessage> {

  /**
   * The listened address.
   *
   * @return the address, not {@code null}
   */
  @CacheReturn
  String address();

  /**
   * Closes the receiver.
   *
   * @param handler handler called when the receiver has been closed, can be {@code null}
   */
  void close(Handler<AsyncResult<Void>> handler);
}
