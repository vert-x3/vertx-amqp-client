package io.vertx.ext.amqp.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.amqp.AmqpReceiver;
import io.vertx.proton.ProtonReceiver;

public class AmqpReceiverImpl implements AmqpReceiver {


  private final String address;
  private final ProtonReceiver receiver;
  private final AmqpConnectionImpl connection;

  public AmqpReceiverImpl(String address, AmqpConnectionImpl connection, ProtonReceiver receiver) {
    this.address = address;
    this.receiver = receiver;
    this.connection = connection;
  }

  @Override
  public String address() {
    return address;
  }

  @Override
  public void close(Handler<AsyncResult<Void>> handler) {
    if (handler == null) {
      handler = x -> {
      };
    }
    connection.unregister(this);
    Future<Void> future = Future.<Void>future().setHandler(handler);
    this.receiver
      .closeHandler(x -> future.handle(x.mapEmpty()))
      .close();

  }
}
