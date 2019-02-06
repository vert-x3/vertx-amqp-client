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

  public AmqpSenderImpl(ProtonSender sender, Context context) {
    this.sender = sender;
    this.context = context;
  }

  @Override
  public AmqpSender send(AmqpMessage message) {
    context.runOnContext(x -> {
      sender.send(message.unwrap());
    });
    return this;
  }

  @Override
  public AmqpSender send(String address, AmqpMessage message) {
    AmqpMessage updated = AmqpMessage.create(message).address(address).build();
    return send(updated);
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
}
