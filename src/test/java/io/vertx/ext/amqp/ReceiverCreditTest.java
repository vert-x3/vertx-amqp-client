package io.vertx.ext.amqp;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.proton.ProtonSession;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class ReceiverCreditTest extends BareTestBase {

  @Test(timeout = 20000)
  public void testInitialCredit(TestContext context) throws Exception {
    doConsumerInitialCreditTestImpl(context, false, 1000);
  }

  @Test(timeout = 20000)
  public void testInitialCreditInfluencedByConsumerBufferSize(TestContext context) throws Exception {
    doConsumerInitialCreditTestImpl(context, true, 42);
  }

  private void doConsumerInitialCreditTestImpl(TestContext context, boolean setMaxBuffered,
                                               int initialCredit) throws Exception {
    final String testName = name.getMethodName();
    final String sentContent = "myMessageContent-" + testName;

    final AtomicBoolean firstSendQDrainHandlerCall = new AtomicBoolean();
    final Async asyncInitialCredit = context.async();
    final Async asyncCompletion = context.async();

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

        serverSender.sendQueueDrainHandler(s -> {
          // Verify the initial credit when the handler is first called and send a message
          if (firstSendQDrainHandlerCall.compareAndSet(false, true)) {
            context.assertEquals(initialCredit, s.getCredit(), "unexpected initial credit");
            context.assertFalse(s.sendQueueFull(), "expected send queue not to be full");

            asyncInitialCredit.complete();

            // send message
            org.apache.qpid.proton.message.Message protonMsg = Proton.message();
            protonMsg.setBody(new AmqpValue(sentContent));

            serverSender.send(protonMsg);
          }
        });

        serverSender.open();
      });
    });

    // === Client consumer handling ====

    AmqpClientOptions options = new AmqpClientOptions().setReplyEnabled(false).setHost("localhost")
      .setPort(server.actualPort());
    if (setMaxBuffered) {
      options.setMaxBufferedMessages(initialCredit);
    }
    client = AmqpClient.create(vertx, options);
    client.connect(res -> {
      context.assertTrue(res.succeeded());
      res.result().receiver(testName, done -> {
        context.assertTrue(done.succeeded());
        AmqpReceiver consumer = done.result();
        consumer.handler(msg -> {
          context.assertNotNull(msg.getBodyAsString(), "amqp message body content was null");
          context.assertEquals(sentContent, msg.getBodyAsString(), "amqp message body not as expected");
          asyncCompletion.complete();
        });
      });
    });

    try {
      asyncInitialCredit.awaitSuccess();
      asyncCompletion.awaitSuccess();
    } finally {
      server.close();
    }
  }
}
