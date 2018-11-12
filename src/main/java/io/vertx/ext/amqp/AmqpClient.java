package io.vertx.ext.amqp;


import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

@VertxGen
public interface AmqpClient {

  /**
   * Connect to the given host and port, without credentials.
   *
   * @param host              the host to connect to
   * @param port              the port to connect to
   * @param connectionHandler handler that will process the result, giving either the connection or failure cause.
   */
  @Fluent
  AmqpClient connect(String host, int port, Handler<AsyncResult<AmqpConnection>> connectionHandler);

  /**
   * Connect to the given host and port, with credentials (if required by server peer).
   *
   * @param host              the host to connect to
   * @param port              the port to connect to
   * @param username          the user name to use in any SASL negotiation that requires it
   * @param password          the password to use in any SASL negotiation that requires it
   * @param connectionHandler handler that will process the result, giving either the connection or failure cause.
   */
  @Fluent
  AmqpClient connect(String host, int port, String username, String password,
                     Handler<AsyncResult<AmqpConnection>> connectionHandler);

  /**
   * Connect to the given host and port, without credentials.
   *
   * @param host              the host to connect to
   * @param port              the port to connect to
   * @param options           the options to apply
   * @param connectionHandler handler that will process the result, giving either the connection or failure cause.
   */
  @Fluent
  AmqpClient connect(String host, int port, AmqpClientOptions options,
                     Handler<AsyncResult<AmqpConnection>> connectionHandler);

  /**
   * Connect to the given host and port, with credentials (if required by server peer).
   *
   * @param options           the options to apply
   * @param host              the host to connect to
   * @param port              the port to connect to
   * @param username          the user name to use in any SASL negotiation that requires it
   * @param password          the password to use in any SASL negotiation that requires it
   * @param connectionHandler handler that will process the result, giving either the connection or failure cause.
   */
  @Fluent
  AmqpClient connect(String host, int port, String username, String password, AmqpClientOptions options,
                     Handler<AsyncResult<AmqpConnection>> connectionHandler);

}
