package io.vertx.ext.amqp.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.amqp.AmqpClientOptions;
import io.vertx.ext.amqp.AmqpLinkOptions;
import io.vertx.ext.amqp.AmqpMessage;
import io.vertx.ext.amqp.AmqpReceiver;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonLinkOptions;
import io.vertx.proton.ProtonReceiver;

import java.util.Objects;

public class AmqpReceiverImpl implements AmqpReceiver {


  private final String address;
  private final ProtonReceiver receiver;

  public AmqpReceiverImpl(String address, ProtonReceiver receiver) {
    this.address = address;
    this.receiver = receiver;
  }

  @Override
  public String address() {
    return address;
  }

  @Override
  public AmqpReceiver close() {
    this.receiver.close();
    this.receiver.detach();
    return this;
  }

  @Override
  public AmqpReceiver close(Handler<AsyncResult<Void>> completionHandler) {
    this.receiver.closeHandler(x -> completionHandler.handle(x.mapEmpty()));
    return close();
  }
}
