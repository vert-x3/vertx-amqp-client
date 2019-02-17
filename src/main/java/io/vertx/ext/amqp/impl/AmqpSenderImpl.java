package io.vertx.ext.amqp.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.amqp.AmqpMessage;
import io.vertx.ext.amqp.AmqpSender;
import io.vertx.proton.ProtonSender;

public class AmqpSenderImpl implements AmqpSender {
  private final ProtonSender sender;
  private final Context context;
  private final AmqpConnectionImpl connection;

  public AmqpSenderImpl(ProtonSender sender, AmqpConnectionImpl connection, Context context) {
    this.sender = sender;
    this.context = context;
    this.connection = connection;
  }

  @Override
  public AmqpSender send(AmqpMessage message) {
    return send(message, null);
  }

  @Override
  public AmqpSender send(AmqpMessage message, Handler<AsyncResult<AmqpMessage>> reply) {
    AmqpMessage updated;
    if (message.address() == null) {
      updated = AmqpMessage.create(message).address(address()).build();
    } else {
      updated = message;
    }
    context.runOnContext(x -> {
      if (reply != null) {
        try {
          connection.replyManager().verify();
        } catch (Exception e) {
          reply.handle(Future.failedFuture(e));
          return;
        }
      }

      // TODO Update credit

      if (reply != null) {
        sender.send(connection.replyManager().registerReplyToHandler(updated, reply).unwrap());
      } else {
        sender.send(updated.unwrap());
      }

      // TODO Update credit
    });
    return this;
  }

  @Override
  public AmqpSender send(String address, AmqpMessage message) {
    AmqpMessage updated = AmqpMessage.create(message).address(address).build();
    return send(updated);
  }

  @Override
  public AmqpSender send(String address, AmqpMessage message, Handler<AsyncResult<AmqpMessage>> reply) {
    AmqpMessage updated = AmqpMessage.create(message).address(address).build();
    return send(updated, reply);
  }

  @Override
  public AmqpSender sendWithAck(AmqpMessage message, Handler<AsyncResult<Void>> acknowledgementHandler) {
    context.runOnContext(x -> {
      sender.send(message.unwrap(), delivery -> {
        switch (delivery.getRemoteState().getType()) {
          case Rejected:
            acknowledgementHandler.handle(Future.failedFuture("message rejected (REJECTED"));
            break;
          case Modified:
            acknowledgementHandler.handle(Future.failedFuture("message rejected (MODIFIED)"));
            break;
          case Released:
            acknowledgementHandler.handle(Future.failedFuture("message rejected (RELEASED)"));
            break;
          case Accepted:
            acknowledgementHandler.handle(Future.succeededFuture());
            break;
          default:
            throw new UnsupportedOperationException("Unsupported delivery type: " + delivery.getRemoteState().getType());
        }
      });
    });

    return this;
  }

  @Override
  public AmqpSender sendWithAck(String address, AmqpMessage message, Handler<AsyncResult<Void>> acknowledgementHandler) {
    AmqpMessage updated = AmqpMessage.create(message).address(address).build();
    return sendWithAck(updated, acknowledgementHandler);
  }

  @Override
  public void close(Handler<AsyncResult<Void>> handler) {
    if (handler == null) {
      handler = x -> {};
    }
    connection.unregister(this);
    if (sender.isOpen()) {
      Future<Void> future = Future.<Void>future().setHandler(handler);
      sender.closeHandler(x -> future.handle(x.mapEmpty()));
      sender.close();
    } else {
      handler.handle(Future.succeededFuture());
    }
  }

  @Override
  public String address() {
    return sender.getRemoteAddress();
  }
}
