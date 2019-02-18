package io.vertx.ext.amqp.impl;

import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.amqp.*;
import org.apache.qpid.proton.amqp.transport.Source;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manage the reception and the emission of reply.
 */
public class ReplyManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReplyManager.class.getName());
  public static final String REPLY_TO_MESSAGE_PROPERTY = "reply-to-message";

  private final AmqpConnectionImpl connection;
  private final Context context;
  private final Vertx vertx;
  private final boolean enabled;
  private final long timeout;

  private AmqpSender sender;
  private AmqpReceiver receiver;
  private String replyToConsumerAddress;

  private Map<String, Handler<AsyncResult<AmqpMessage>>> replyToHandler = new ConcurrentHashMap<>();

  public ReplyManager(Vertx vertx, Context context, AmqpConnectionImpl connection, boolean replyEnabled, long replyTimeout) {
    this.vertx = vertx;
    this.context = context;
    this.connection = connection;
    this.enabled = replyEnabled;
    this.timeout = replyTimeout;
  }

  public void sendReply(AmqpMessage origin, AmqpMessage response) {
    sendReply(origin, response, null);
  }

  public void sendReply(AmqpMessage origin, AmqpMessage response, Handler<AsyncResult<AmqpMessage>> replyToReply) {
    if (!isReplySupported()) {
      throw new IllegalStateException("Unable to send reply, reply disabled");
    }
    AmqpMessageBuilder builder = AmqpMessage.create(response);
    String replyAddress = origin.replyTo();
    if (replyAddress == null) {
      throw new IllegalArgumentException("Original message has no reply-to address, unable to send implicit reply");
    } else {
      builder.address(replyAddress);
    }

    String origMessageId = origin.correlationId();
    if (origMessageId != null) {
      builder.applicationProperties(new JsonObject().put(REPLY_TO_MESSAGE_PROPERTY, origMessageId));
    }

    if (replyToReply != null) {
      sender.send(builder.build(), replyToReply);
    } else {
      sender.send(builder.build());
    }
  }

  public boolean isReplySupported() {
    return enabled && connection.unwrap().isAnonymousRelaySupported();
  }

  public Future<Void> initialize() {
    if (!isReplySupported()) {
      return Future.succeededFuture();
    }
    Future<Void> senderFuture = Future.future();
    Future<Void> receiverFuture = Future.future();

    this.connection.sender(null, done -> {
      if (done.failed()) {
        senderFuture.tryFail(done.cause());
      } else {
        this.sender = done.result();
        senderFuture.tryComplete();
      }
    });

    this.connection.receiver(null, new AmqpReceiverOptions().setDynamic(true),
      this::handleIncomingMessageReply,
      done -> {
        this.receiver = done.result();
        if (done.failed()) {
          receiverFuture.tryFail(done.cause());
        } else {
          Source remoteSource = ((AmqpReceiverImpl) this.receiver).unwrap().getRemoteSource();
          if (remoteSource != null) {
            this.replyToConsumerAddress = remoteSource.getAddress();
          }
          receiverFuture.tryComplete();
        }
      });
    Future<Void> future = Future.future();
    CompositeFuture.join(senderFuture, receiverFuture).setHandler(ar -> {
      if (ar.failed()) {
        if (senderFuture.failed() && !receiverFuture.failed()) {
          this.receiver.close(x -> {
          });
        }
        if (receiverFuture.failed() && !senderFuture.failed()) {
          this.sender.close(x -> {
          });
        }
        future.fail(ar.cause());
      } else {
        future.complete();
      }
    });

    return future;
  }

  void verify() throws Exception {
    if (!isReplySupported()) {
      throw new Exception("Reply management disabled");
    }

    if (replyToConsumerAddress == null) {
      throw new Exception(
        "No reply-to address available, unable to send with a reply handler. Try an explicit consumer for replies.");
    }
  }

  AmqpMessage registerReplyToHandler(AmqpMessage message,
                                     Handler<AsyncResult<AmqpMessage>> replyHandler) {
    try {
      verify();
    } catch (Exception e) {
      replyHandler.handle(Future.failedFuture(e));
      return null;
    }

    String correlationId = UUID.randomUUID().toString();
    AmqpMessage updated = AmqpMessage.create(message)
      .replyTo(replyToConsumerAddress)
      .correlationId(correlationId)
      .build();

    if (replyToHandler.containsKey(correlationId)) {
      throw new IllegalStateException("Already a correlation id: " + correlationId);
    }
    replyToHandler.put(correlationId, replyHandler);

    vertx.setTimer(timeout, x -> {
      Handler<AsyncResult<AmqpMessage>> handler = replyToHandler.remove(correlationId);
      if (handler != null) {
        handler.handle(Future.failedFuture("Timeout waiting for reply"));
      }
    });

    return updated;
  }


  public Future close() {
    Future<Void> f1 = Future.future();
    Future<Void> f2 = Future.future();
    if (sender != null) {
      sender.close(x -> {
        f1.handle(x.mapEmpty());
      });
    } else {
      f1.complete();
    }
    if (receiver != null) {
      receiver.close(x -> {
        f2.handle(x.mapEmpty());
      });
    } else {
      f2.complete();
    }
    return CompositeFuture.all(f1, f2);
  }

  private void handleIncomingMessageReply(AmqpMessage response) {
    // Check if we have the reply-to-message app properties
    String correlationId = response.applicationProperties().getString(REPLY_TO_MESSAGE_PROPERTY, response.correlationId());
    if (correlationId != null) {
      // Remove the associated handler from the map (only 1 reply permitted).
      Handler<AsyncResult<AmqpMessage>> handler = replyToHandler.remove(correlationId);

      if (handler != null) {
        handler.handle(Future.succeededFuture(response));
      } else {
        // The handler has already been removed - timeout.
        LOGGER.warn("A response has been received, but after the deadline");
      }
    } else {
      LOGGER.error("Received message on replyTo consumer, could not match to a replyHandler: " + response);
    }
  }

}
