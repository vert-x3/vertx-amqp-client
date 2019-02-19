package io.vertx.ext.amqp;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.apache.qpid.proton.amqp.Symbol;


/**
 * Once connected to the broker or router, you get a connection. This connection is automatically opened.
 */
@VertxGen
public interface AmqpConnection {

  void endHandler(Handler<Void> endHandler);

  /**
   * Closes the AMQP connection, i.e. allows the Close frame to be emitted.
   * <p>
   * For locally initiated connections, the {@link #closeHandler(Handler)} may be used to handle the peer sending their
   * Close frame (if they haven't already).
   *
   * @param done the close handler notified when the connection is closed. May be {@code null}.
   * @return the connection
   */
  @Fluent
  AmqpConnection close(Handler<AsyncResult<Void>> done);

  @Fluent
  AmqpConnection receiver(String address, Handler<AsyncResult<AmqpReceiver>> completionHandler);

  /**
   * Creates a receiver used to consumer messages from the given node address.
   *
   * @param address           The source address to attach the consumer to, must not be {@code null}
   * @param messageHandler    The message handler, must not be {@code null}
   * @param completionHandler the handler called with the receiver has been opened
   * @return the connection.
   */
  @Fluent
  AmqpConnection receiver(String address, Handler<AmqpMessage> messageHandler, Handler<AsyncResult<AmqpReceiver>> completionHandler);

  /**
   * Creates a receiver used to consumer messages from the given node address.
   *
   * @param address           The source address to attach the consumer to.
   * @param receiverOptions   The options for this receiver.
   * @param messageHandler    The message handler, must not be {@code null}
   * @param completionHandler The handler called with the receiver, once opened
   * @return the connection.
   */
  @Fluent
  AmqpConnection receiver(String address, AmqpReceiverOptions receiverOptions, Handler<AmqpMessage> messageHandler,
                          Handler<AsyncResult<AmqpReceiver>> completionHandler);

  /**
   * Creates a sender used to send messages to the given node address. If no address (i.e null) is specified then a
   * sender will be established to the 'anonymous relay' and each message must specify its destination address.
   *
   * @param address           The target address to attach to, or null to attach to the anonymous relay.
   * @param completionHandler The handler called with the sender, once opened
   * @return the connection.
   */
  @Fluent
  AmqpConnection sender(String address, Handler<AsyncResult<AmqpSender>> completionHandler);

  /**
   * Creates a sender used to send messages to the given node address. If no address (i.e null) is specified then a
   * sender will be established to the 'anonymous relay' and each message must specify its destination address.
   *
   * @param address           The target address to attach to, or null to attach to the anonymous relay.
   * @param senderOptions     The options for this sender.
   * @param completionHandler The handler called with the sender, once opened
   * @return the connection.
   */
  @Fluent
  AmqpConnection sender(String address, AmqpLinkOptions senderOptions, Handler<AsyncResult<AmqpSender>> completionHandler);

  /**
   * Sets a handler for when an AMQP Close frame is received from the remote peer.
   *
   * @param remoteCloseHandler the handler
   * @return the connection
   */
  @Fluent
  AmqpConnection closeHandler(Handler<AmqpConnection> remoteCloseHandler);

}
