package io.vertx.ext.amqp.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.amqp.AmqpMessage;
import io.vertx.ext.amqp.AmqpSender;
import io.vertx.proton.ProtonSender;
import org.apache.qpid.proton.amqp.transport.DeliveryState;

public class AmqpSenderImpl implements AmqpSender {
  private final ProtonSender sender;

  public AmqpSenderImpl(ProtonSender sender) {
    this.sender = sender;
  }

  @Override
  public AmqpSender send(AmqpMessage message) {
    sender.send(message.unwrap());
    return this;
  }

  @Override
  public AmqpSender send(String address, AmqpMessage message) {
    AmqpMessage updated = AmqpMessage.create(message).address(address).build();
    return send(updated);
  }

  @Override
  public AmqpSender sendWithAck(AmqpMessage message, Handler<AsyncResult<Void>> acknowledgementHandler) {
    sender.send(message.unwrap(), delivery -> {
      if (delivery.getRemoteState().getType() == DeliveryState.DeliveryStateType.Rejected) {
        acknowledgementHandler.handle(Future.failedFuture("rejected"));
      } else if (delivery.getRemoteState().getType() == DeliveryState.DeliveryStateType.Accepted) {
        acknowledgementHandler.handle(Future.succeededFuture());
      }
    });
    return this;
  }

  @Override
  public AmqpSender sendWithAck(String address, AmqpMessage message, Handler<AsyncResult<Void>> acknowledgementHandler) {
    AmqpMessage updated = AmqpMessage.create(message).address(address).build();
    return sendWithAck(updated, acknowledgementHandler);
  }
}
