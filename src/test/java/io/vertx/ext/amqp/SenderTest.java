package io.vertx.ext.amqp;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.proton.ProtonHelper;
import io.vertx.proton.ProtonSession;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.Target;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicBoolean;

public class SenderTest extends BareTestBase {

  @Test(timeout = 20000)
  public void testProducerClose(TestContext context) throws Exception {
    doProducerCloseTestImpl(context, false);
  }

  @Test(timeout = 20000)
  public void testProducerEnd(TestContext context) throws Exception {
    doProducerCloseTestImpl(context, true);
  }

  private void doProducerCloseTestImpl(TestContext context, boolean callEnd) throws Exception {
    final String testName = name.getMethodName();
    final String sentContent = "myMessageContent-" + testName;

    final Async asyncReceiveMsg = context.async();
    final Async asyncClose = context.async();
    final Async asyncShutdown = context.async();

    final AtomicBoolean exceptionHandlerCalled = new AtomicBoolean();

    // === Server handling ====

    MockServer server = new MockServer(vertx, serverConnection -> {
      // Expect a connection
      serverConnection.openHandler(serverSender -> {
        // Add a close handler
        serverConnection.closeHandler(x -> serverConnection.close());
        serverConnection.open();
      });

      // Expect a session to open, when the producer is created
      serverConnection.sessionOpenHandler(ProtonSession::open);

      // Expect a receiver link open for the producer
      serverConnection.receiverOpenHandler(serverReceiver -> {
        Target remoteTarget = serverReceiver.getRemoteTarget();
        context.assertNotNull(remoteTarget, "target should not be null");
        context.assertEquals(testName, remoteTarget.getAddress(), "expected given address");
        // Naive test-only handling
        serverReceiver.setTarget(remoteTarget.copy());

        // Add the message handler
        serverReceiver.handler((delivery, message) -> {
          Section body = message.getBody();
          context.assertNotNull(body, "received body was null");
          context.assertTrue(body instanceof AmqpValue, "unexpected body section type: " + body.getClass());
          context.assertEquals(sentContent, ((AmqpValue) body).getValue(), "Unexpected message body content");

          asyncReceiveMsg.complete();
        });

        // Add a close handler
        serverReceiver.closeHandler(x -> {
          serverReceiver.close();
          asyncClose.complete();
        });

        serverReceiver.open();
      });
    });

    // === Client producer handling ====

    AmqpClientOptions options = new AmqpClientOptions().setReplyEnabled(false)
      .setHost("localhost").setPort(server.actualPort());
    AmqpClient client = AmqpClient.create(vertx, options);
    client.connect(res -> {
      // Set up a producer using the client, use it, close it.
      context.assertTrue(res.succeeded());

      res.result().sender(testName, done -> {
        AmqpSender sender = done.result();
        sender.exceptionHandler(x -> exceptionHandlerCalled.set(true));
        sender.sendWithAck(AmqpMessage.create().body(sentContent).build(), x -> {
          context.assertTrue(x.succeeded());

          if (callEnd) {
            sender.end();
          } else {
            sender.close(null);
          }

          client.close(shutdownRes -> {
            context.assertTrue(shutdownRes.succeeded());
            asyncShutdown.complete();
          });
        });
      });
    });

    try {
      asyncReceiveMsg.awaitSuccess();
      asyncClose.awaitSuccess();
      asyncShutdown.awaitSuccess();
      context.assertFalse(exceptionHandlerCalled.get(), "exception handler unexpectedly called");
    } finally {
      server.close();
    }
  }

  @Test(timeout = 20000)
  public void testSenderFlowControlMechanisms(TestContext context) throws Exception {
    final long delay = 250;
    final String testName = name.getMethodName();
    final String sentContent = "myMessageContent-" + testName;

    final Async asyncSendInitialCredit = context.async();
    final Async asyncSendSubsequentCredit = context.async();
    final Async asyncShutdown = context.async();

    // === Server handling ====

    MockServer server = new MockServer(vertx, serverConnection -> {
      // Expect a connection
      serverConnection.openHandler(serverSender -> {
        // Add a close handler
        serverConnection.closeHandler(x -> serverConnection.close());
        serverConnection.open();
      });

      // Expect a session to open, when the sender is created
      serverConnection.sessionOpenHandler(ProtonSession::open);

      // Expect a receiver link open for the sender
      serverConnection.receiverOpenHandler(serverReceiver -> {
        Target remoteTarget = serverReceiver.getRemoteTarget();
        context.assertNotNull(remoteTarget, "target should not be null");
        context.assertEquals(testName, remoteTarget.getAddress(), "expected given address");
        // Naive test-only handling
        serverReceiver.setTarget(remoteTarget.copy());

        // Disable auto accept and credit prefetch handling, do it (or not) ourselves
        serverReceiver.setAutoAccept(false);
        serverReceiver.setPrefetch(0);

        // Add the message handler
        serverReceiver.handler((delivery, message) -> {
          Section body = message.getBody();
          context.assertNotNull(body, "received body was null");
          context.assertTrue(body instanceof AmqpValue, "unexpected body section type: " + body.getClass());
          context.assertEquals(sentContent, ((AmqpValue) body).getValue(), "Unexpected message body content");

          // Only flow subsequent credit after a delay and related checks complete
          vertx.setTimer(delay, x -> {
            asyncSendSubsequentCredit.awaitSuccess();
            serverReceiver.flow(1);
          });
        });

        // Add a close handler
        serverReceiver.closeHandler(x -> serverReceiver.close());
        serverReceiver.open();

        // Only flow initial credit after a delay and initial checks complete
        vertx.setTimer(delay, x -> {
          asyncSendInitialCredit.awaitSuccess();
          serverReceiver.flow(1);
        });
      });
    });

    // === Client producer handling ====

    AmqpClientOptions options = new AmqpClientOptions().setReplyEnabled(false)
      .setHost("localhost")
      .setPort(server.actualPort());
    AmqpClient client = AmqpClient.create(vertx, options);
    client.connect(res -> {
      context.assertTrue(res.succeeded());
      res.result().sender(testName, done -> {
        context.assertTrue(done.succeeded());
        AmqpSender sender = done.result();
        context.assertTrue(sender.writeQueueFull(), "expected write queue to be full, we have not yet granted credit");
        sender.drainHandler(x -> {
          context.assertTrue(asyncSendInitialCredit.isSucceeded(), "should have been called after initial credit delay");
          context.assertFalse(sender.writeQueueFull(), "expected write queue not to be full, we just granted credit");

          // Send message using the credit
          sender.send(AmqpMessage.create().body(sentContent).build());
          context.assertTrue(sender.writeQueueFull(), "expected write queue to be full, we just used all the credit");

          // Now replace the drain handler, have it act on subsequent credit arriving
          sender.drainHandler(y -> {
            context.assertTrue(asyncSendSubsequentCredit.isSucceeded(), "should have been called after 2nd credit delay");
            context.assertFalse(sender.writeQueueFull(), "expected write queue not to be full, we just granted credit");

            client.close(shutdownRes -> {
              context.assertTrue(shutdownRes.succeeded());
              asyncShutdown.complete();
            });
          });
          // Now allow server to send the subsequent credit
          asyncSendSubsequentCredit.complete();
        });
      });
      // Now allow to send initial credit. Things will kick off again in the drain handler.
      asyncSendInitialCredit.complete();
    });

    try {
      asyncShutdown.awaitSuccess();
    } finally {
      server.close();
    }
  }

  @Test(timeout = 20000)
  public void testSenderClosedRemotelyCallsExceptionHandler(TestContext context) throws Exception {
    doSenderClosedRemotelyCallsExceptionHandlerTestImpl(context, false);
  }

  @Test(timeout = 20000)
  public void testSenderClosedRemotelyWithErrorCallsExceptionHandler(TestContext context) throws Exception {
    doSenderClosedRemotelyCallsExceptionHandlerTestImpl(context, true);
  }

  private void doSenderClosedRemotelyCallsExceptionHandlerTestImpl(TestContext context,
                                                                   boolean closeWithError) throws Exception {
    final String testName = name.getMethodName();
    final String sentContent = "myMessageContent-" + testName;

    final Async asyncShutdown = context.async();
    final Async asyncExceptionHandlerCalled = context.async();

    // === Server handling ====

    MockServer server = new MockServer(vertx, serverConnection -> {
      // Expect a connection
      serverConnection.openHandler(serverSender -> {
        // Add a close handler
        serverConnection.closeHandler(x -> serverConnection.close());
        serverConnection.open();
      });

      // Expect a session to open, when the sender is created
      serverConnection.sessionOpenHandler(ProtonSession::open);

      // Expect a receiver link open for the sender
      serverConnection.receiverOpenHandler(serverReceiver -> {
        Target remoteTarget = serverReceiver.getRemoteTarget();
        context.assertNotNull(remoteTarget, "target should not be null");
        context.assertEquals(testName, remoteTarget.getAddress(), "expected given address");
        // Naive test-only handling
        serverReceiver.setTarget(remoteTarget.copy());

        // Add the message handler
        serverReceiver.handler((delivery, message) -> {
          Section body = message.getBody();
          context.assertNotNull(body, "received body was null");
          context.assertTrue(body instanceof AmqpValue, "unexpected body section type: " + body.getClass());
          context.assertEquals(sentContent, ((AmqpValue) body).getValue(), "Unexpected message body content");

          if (closeWithError) {
            serverReceiver.setCondition(ProtonHelper.condition(AmqpError.INTERNAL_ERROR, "testing-error"));
          }

          // Now close the link server side
          serverReceiver.close();
        });

        // Add a close handler
        serverReceiver.closeHandler(x -> serverReceiver.close());
        serverReceiver.open();
      });
    });

    // === Client producer handling ====

    AmqpClientOptions options = new AmqpClientOptions().setReplyEnabled(false)
      .setHost("localhost").setPort(server.actualPort());
    client = AmqpClient.create(vertx, options);
    client.connect(res -> {
      context.assertTrue(res.succeeded());
      res.result().sender(testName, done -> {
        context.assertTrue(done.succeeded());
        AmqpSender sender = done.result();
        sender.exceptionHandler(ex -> {
          context.assertNotNull(ex, "expected exception");
          context.assertTrue(ex instanceof Exception, "expected vertx exception");
          if (closeWithError) {
            context.assertNotNull(ex.getCause(), "expected cause");
          } else {
            context.assertNull(ex.getCause(), "expected no cause");
          }
          asyncExceptionHandlerCalled.complete();

          client.close(shutdownRes -> {
            if (shutdownRes.failed()) {
              shutdownRes.cause().printStackTrace();
            }
            context.assertTrue(shutdownRes.succeeded());
            asyncShutdown.complete();
          });
        });
        sender.send(AmqpMessage.create().body(sentContent).build());
      });
    });

    try {
      asyncExceptionHandlerCalled.awaitSuccess();
      asyncShutdown.awaitSuccess();
    } finally {
      server.close();
    }
  }
}
