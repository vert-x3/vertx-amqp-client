package io.vertx.ext.amqp.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.amqp.AmqpMessage;
import io.vertx.ext.amqp.AmqpSender;
import io.vertx.proton.ProtonDelivery;
import io.vertx.proton.ProtonSender;
import io.vertx.proton.impl.ProtonSenderImpl;

public class AmqpSenderImpl implements AmqpSender {
  private final ProtonSender sender;
  private final Context context;
  private final AmqpConnectionImpl connection;
  private boolean closed;
  private Handler<Throwable> exceptionHandler;
  private Handler<Void> drainHandler;
  private long remoteCredit = 0;

  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpSender.class);

  public AmqpSenderImpl(ProtonSender sender, AmqpConnectionImpl connection, Context context) {
    this.sender = sender;
    this.context = context;
    this.connection = connection;

    sender.closeHandler(res -> {
      Handler<Throwable> eh = null;
      boolean closeSender = false;

      synchronized (AmqpSenderImpl.this) {
        if (!closed && exceptionHandler != null) {
          eh = exceptionHandler;
        }

        if(!closed) {
          closed = true;
          closeSender = true;
        }
      }

      if (eh != null) {
        if (res.succeeded()) {
          eh.handle(new Exception("Sender closed remotely"));
        } else {
          eh.handle(new Exception("Sender closed remotely with error", res.cause()));
        }
      }

      if(closeSender) {
        sender.close();
      }
    });
    sender.sendQueueDrainHandler(s -> {
      Handler<Void> dh = null;
      synchronized (AmqpSenderImpl.this) {
        // Update current state of remote credit
        remoteCredit = ((ProtonSenderImpl) sender).getRemoteCredit();

        // check the user drain handler, fire it outside synchronized block if not null
        if (drainHandler != null) {
          dh = drainHandler;
        }
      }

      if(dh != null) {
        dh.handle(null);
      }
    });

    sender.open();
  }

  @Override
  public synchronized boolean writeQueueFull() {
    return remoteCredit <= 0;
  }

  @Override
  public AmqpSender send(AmqpMessage message) {
    return send(message, null);
  }

  @Override
  public AmqpSender send(AmqpMessage message, Handler<AsyncResult<AmqpMessage>> reply) {
    return doSend(message, reply, null);
  }

  private AmqpSender doSend(AmqpMessage message, Handler<AsyncResult<AmqpMessage>> reply,
                            Handler<AsyncResult<Void>> acknowledgmentHandler) {
    AmqpMessage updated;
    if (message.address() == null) {
      updated = AmqpMessage.create(message).address(address()).build();
    } else {
      updated = message;
    }

    Handler<ProtonDelivery> ack = delivery -> {
      Handler<AsyncResult<Void>> handler = acknowledgmentHandler;
      if (acknowledgmentHandler == null) {
        handler = ar -> {
          if (ar.failed()) {
            LOGGER.warn("Message rejected by remote peer", ar.cause());
          }
        };
      }

      switch (delivery.getRemoteState().getType()) {
        case Rejected:
          handler.handle(Future.failedFuture("message rejected (REJECTED"));
          break;
        case Modified:
          handler.handle(Future.failedFuture("message rejected (MODIFIED)"));
          break;
        case Released:
          handler.handle(Future.failedFuture("message rejected (RELEASED)"));
          break;
        case Accepted:
          handler.handle(Future.succeededFuture());
          break;
        default:
          handler.handle(Future.failedFuture("Unsupported delivery type: " + delivery.getRemoteState().getType()));
      }
    };

    context.runOnContext(x -> {
      if (reply != null) {
        try {
          connection.replyManager().verify();
        } catch (Exception e) {
          reply.handle(Future.failedFuture(e));
          return;
        }
      }

      synchronized (AmqpSenderImpl.this) {
        // Update the credit tracking. We only need to adjust this here because the sends etc may not be on the context
        // thread and if that is the case we can't use the ProtonSender sendQueueFull method to check that credit has been
        // exhausted following this doSend call since we will have only scheduled the actual send for later.
        remoteCredit--;
      }

      if (reply != null) {
        sender.send(connection.replyManager().registerReplyToHandler(updated, reply).unwrap(), ack);
      } else {
        sender.send(updated.unwrap(), ack);
      }

      synchronized (AmqpSenderImpl.this) {
        // Update the credit tracking *again*. We need to reinitialise it here in case the doSend call was performed on
        // a thread other than the bridge context, to ensure we didn't fall foul of a race between the above pre-send
        // update on that thread, the above send on the context thread, and the sendQueueDrainHandler based updates on
        // the context thread.
        remoteCredit = ((ProtonSenderImpl) sender).getRemoteCredit();
      }
    });
    return this;
  }

  @Override
  public synchronized AmqpSender exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = handler;
    return this;
  }

  @Override
  public AmqpSender write(AmqpMessage data) {
    return send(data, null);
  }

  @Override
  public AmqpSender setWriteQueueMaxSize(int maxSize) {
    // No-op, available sending credit is controlled by recipient peer in AMQP 1.0.
    return this;
  }

  @Override
  public void end() {
    close(null);
  }

  @Override
  public synchronized AmqpSender drainHandler(Handler<Void> handler) {
    drainHandler = handler;
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
    return doSend(message, null, acknowledgementHandler);
  }

  @Override
  public AmqpSender sendWithAck(String address, AmqpMessage message, Handler<AsyncResult<Void>> acknowledgementHandler) {
    AmqpMessage updated = AmqpMessage.create(message).address(address).build();
    return sendWithAck(updated, acknowledgementHandler);
  }

  @Override
  public void close(Handler<AsyncResult<Void>> handler) {
    synchronized (this) {
      closed = true;
    }
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
