package io.vertx.ext.amqp;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.proton.ProtonHelper;
import io.vertx.proton.ProtonSession;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class ClosingReceiverTest extends BareTestBase {

  @Test(timeout = 20000)
  public void testConsumerClosedRemotelyCallsExceptionHandler(TestContext context) throws Exception {
    doConsumerClosedRemotelyCallsExceptionHandlerTestImpl(context, false);
  }

  @Test(timeout = 20000)
  public void testConsumerClosedRemotelyWithErrorCallsExceptionHandler(TestContext context) throws Exception {
    doConsumerClosedRemotelyCallsExceptionHandlerTestImpl(context, true);
  }

  private void doConsumerClosedRemotelyCallsExceptionHandlerTestImpl(TestContext context,
                                                                     boolean closeWithError) throws Exception {

    final String testName = name.getMethodName();
    final String sentContent = "myMessageContent-" + testName;

    final Async asyncShutdown = context.async();
    final Async asyncExceptionHandlerCalled = context.async();

    final AtomicBoolean msgReceived = new AtomicBoolean();

    // === Server handling ====

    MockServer server = new MockServer(vertx, serverConnection -> {
      // Expect a connection
      serverConnection.openHandler(serverSender -> {
        // Add a close handler
        serverConnection.closeHandler(x -> serverConnection.close());
        serverConnection.open();
      });

      // Expect a session to open, when the receiver is created
      serverConnection.sessionOpenHandler(ProtonSession::open);

      // Expect a sender link open for the receiver
      serverConnection.senderOpenHandler(serverSender -> {
        Source remoteSource = (Source) serverSender.getRemoteSource();
        context.assertNotNull(remoteSource, "source should not be null");
        context.assertEquals(testName, remoteSource.getAddress(), "expected given address");
        // Naive test-only handling
        serverSender.setSource(remoteSource.copy());

        serverSender.open();

        // Assume we will get credit, buffer the send immediately
        org.apache.qpid.proton.message.Message protonMsg = Proton.message();
        protonMsg.setBody(new AmqpValue(sentContent));

        // Delay the message, or the connection will be marked as failed immediately.
        vertx.setTimer(500, x -> {
          serverSender.send(protonMsg);
          // Mark it closed server side
          if (closeWithError) {
            serverSender.setCondition(ProtonHelper.condition(AmqpError.INTERNAL_ERROR, "testing-error"));
          }
          serverSender.close();

        });
      });
    });

    // === Client consumer handling ====

    AmqpClientOptions options = new AmqpClientOptions().setReplyEnabled(false).setHost("localhost").setPort(server.actualPort());
    client = AmqpClient.create(vertx, options);
    this.client.connect(res -> {
      context.assertTrue(res.succeeded());
      res.result().receiver(testName,
        msg -> {
          String content = msg.getBodyAsString();
          context.assertNotNull(content, "amqp message body content was null");
          context.assertEquals(sentContent, content, "amqp message body not as expected");
          msgReceived.set(true);
        },
        done -> {
          context.assertTrue(done.succeeded());
          AmqpReceiver receiver = done.result();
          receiver.exceptionHandler(ex -> {
            context.assertNotNull(ex, "expected exception");
            context.assertTrue(ex instanceof Exception, "expected exception");
            if (closeWithError) {
              context.assertNotNull(ex.getCause(), "expected cause");
            } else {
              context.assertNull(ex.getCause(), "expected no cause");
            }

            context.assertTrue(msgReceived.get(), "expected msg to be received first");
            asyncExceptionHandlerCalled.complete();

            client.close(shutdownRes -> {
              context.assertTrue(shutdownRes.succeeded());
              asyncShutdown.complete();
            });
          });
        });
    });

    try {
      asyncExceptionHandlerCalled.awaitSuccess();
      asyncShutdown.awaitSuccess();
    } finally {
      server.close();
    }
  }

  @Test(timeout = 20000)
  public void testConsumerClosedRemotelyCallsEndHandler(TestContext context) throws Exception {
    final String testName = name.getMethodName();
    final String sentContent = "myMessageContent-" + testName;

    final Async asyncShutdown = context.async();
    final Async asyncEndHandlerCalled = context.async();

    final AtomicBoolean msgReceived = new AtomicBoolean();

    // === Server handling ====

    MockServer server = new MockServer(vertx, serverConnection -> {
      // Expect a connection
      serverConnection.openHandler(serverSender -> {
        // Add a close handler
        serverConnection.closeHandler(x -> {
          serverConnection.close();
        });

        serverConnection.open();
      });

      // Expect a session to open, when the receiver is created
      serverConnection.sessionOpenHandler(ProtonSession::open);

      // Expect a sender link open for the receiver
      serverConnection.senderOpenHandler(serverSender -> {
        Source remoteSource = (Source) serverSender.getRemoteSource();
        context.assertNotNull(remoteSource, "source should not be null");
        context.assertEquals(testName, remoteSource.getAddress(), "expected given address");
        // Naive test-only handling
        serverSender.setSource(remoteSource.copy());

        serverSender.open();

        vertx.setTimer(500, x -> {
          // Assume we will get credit, buffer the send immediately
          org.apache.qpid.proton.message.Message protonMsg = Proton.message();
          protonMsg.setBody(new AmqpValue(sentContent));

          serverSender.send(protonMsg);

          // Mark it closed server side
          serverSender.setCondition(ProtonHelper.condition(AmqpError.INTERNAL_ERROR, "testing-error"));

          serverSender.close();
        });
      });
    });

    // === Client consumer handling ====

    AmqpClientOptions options = new AmqpClientOptions().setReplyEnabled(false)
      .setHost("localhost").setPort(server.actualPort());
    client = AmqpClient.create(vertx, options);
    client.connect(res -> {
      context.assertTrue(res.succeeded());
      res.result().receiver(testName,
        msg -> {
          context.assertNotNull(msg.getBodyAsString(), "message body was null");
          String amqpBodyContent = msg.getBodyAsString();
          context.assertNotNull(amqpBodyContent, "amqp message body content was null");
          context.assertEquals(sentContent, amqpBodyContent, "amqp message body not as expected");

          msgReceived.set(true);
        }, done -> {
          context.assertTrue(done.succeeded());
          AmqpReceiver consumer = done.result();
          consumer.endHandler(x -> {
            context.assertTrue(msgReceived.get(), "expected msg to be received first");
            asyncEndHandlerCalled.complete();
            client.close(shutdownRes -> {
              context.assertTrue(shutdownRes.succeeded());
              asyncShutdown.complete();
            });
          });
        });
    });

    try {
      asyncEndHandlerCalled.awaitSuccess();
      asyncShutdown.awaitSuccess();
    } finally {
      server.close();
    }
  }

  @Test(timeout = 20000)
  public void testConsumerClosedLocallyDoesNotCallEndHandler(TestContext context) throws Exception {
    final String testName = name.getMethodName();
    final String sentContent = "myMessageContent-" + testName;

    final Async asyncShutdown = context.async();

    // === Server handling ====

    MockServer server = new MockServer(vertx, serverConnection -> {
      // Expect a connection
      serverConnection.openHandler(serverSender -> {
        // Add a close handler
        serverConnection.closeHandler(x -> serverConnection.close());
        serverConnection.open();
      });

      // Expect a session to open, when the receiver is created
      serverConnection.sessionOpenHandler(ProtonSession::open);

      // Expect a sender link open for the receiver
      serverConnection.senderOpenHandler(serverSender -> {
        Source remoteSource = (Source) serverSender.getRemoteSource();
        context.assertNotNull(remoteSource, "source should not be null");
        context.assertEquals(testName, remoteSource.getAddress(), "expected given address");
        // Naive test-only handling
        serverSender.setSource(remoteSource.copy());

        vertx.setTimer(500, x -> {
          // Assume we will get credit, buffer the send immediately
          org.apache.qpid.proton.message.Message protonMsg = Proton.message();
          protonMsg.setBody(new AmqpValue(sentContent));
          serverSender.send(protonMsg);
          // Add a close handler
          serverSender.closeHandler(y -> serverSender.close());
        });

        serverSender.open();
      });
    });

    // === Bridge consumer handling ====

    AmqpClientOptions options = new AmqpClientOptions().setReplyEnabled(false)
      .setPort(server.actualPort()).setHost("localhost");
    client = AmqpClient.create(vertx, options);
    client.connect(res -> {
      context.assertTrue(res.succeeded());
      res.result().receiver(testName,
        done -> {
          context.assertTrue(done.succeeded());
          AmqpReceiver consumer = done.result();
          consumer.endHandler(x -> context.fail("should not call end handler"));
          // Attach handler.
          consumer.handler(msg -> {
            context.assertNotNull(msg.getBodyAsString(), "message body was null");
            context.assertEquals(sentContent, msg.getBodyAsString(), "amqp message body not as expected");

            consumer.close(x -> {
              context.assertTrue(x.succeeded());
              // closing complete, schedule shutdown, give chance for end handler to run, so we can verify it didn't.
              vertx.setTimer(50, y -> {
                client.close(shutdownRes -> {
                  context.assertTrue(shutdownRes.succeeded());
                  asyncShutdown.complete();
                });
              });
            });
          });
        });
    });

    try {
      asyncShutdown.awaitSuccess();
    } finally {
      server.close();
    }
  }
}
