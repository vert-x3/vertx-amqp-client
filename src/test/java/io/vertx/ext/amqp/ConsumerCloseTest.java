package io.vertx.ext.amqp;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonSession;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerCloseTest extends BareTestBase {

  @Test(timeout = 20000)
  public void testConsumerCloseCompletionNotification(TestContext context) throws Exception {
    final String testName = name.getMethodName();
    final String sentContent = "myMessageContent-" + testName;

    final Async asyncUnregister = context.async();
    final Async asyncShutdown = context.async();

    final AtomicBoolean exceptionHandlerCalled = new AtomicBoolean();

    MockServer server = new MockServer(vertx,
      serverConnection -> handleReceiverOpenSendMessageThenClose(serverConnection, testName, sentContent, context));

    AmqpClientOptions options = new AmqpClientOptions().setReplyEnabled(false)
      .setHost("localhost").setPort(server.actualPort());
    AmqpClient client = AmqpClient.create(vertx, options);
    client.connect(res -> {
      context.assertTrue(res.succeeded());
      res.result().receiver(testName, done -> {
        context.assertTrue(done.succeeded());
        AmqpReceiver receiver = done.result();
        receiver.exceptionHandler(x -> exceptionHandlerCalled.set(true));
        receiver.handler(msg -> {
          String amqpBodyContent = msg.getBodyAsString();
          context.assertNotNull(amqpBodyContent, "amqp message body content was null");
          context.assertEquals(sentContent, amqpBodyContent, "amqp message body not as expected");

          receiver.close(x -> {
            context.assertTrue(x.succeeded(), "Expected close to succeed");
            asyncUnregister.complete();

            client.close(shutdownRes -> {
              context.assertTrue(shutdownRes.succeeded());
              asyncShutdown.complete();
            });
          });
        });

      });
    });

    try {
      context.assertFalse(exceptionHandlerCalled.get(), "exception handler unexpectedly called");
      asyncUnregister.awaitSuccess();
      asyncShutdown.awaitSuccess();
    } finally {
      server.close();
    }
  }

  private void handleReceiverOpenSendMessageThenClose(ProtonConnection serverConnection, String testAddress,
                                                      String testContent, TestContext context) {
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
      context.assertEquals(testAddress, remoteSource.getAddress(), "expected given address");
      // Naive test-only handling
      serverSender.setSource(remoteSource.copy());

      // Assume we will get credit, buffer the send immediately
      org.apache.qpid.proton.message.Message protonMsg = Proton.message();
      protonMsg.setBody(new AmqpValue(testContent));

      serverSender.send(protonMsg);

      // Add a close handler
      serverSender.closeHandler(x -> serverSender.close());
      serverSender.open();
    });
  }
}
