package io.vertx.ext.amqp;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.UUID;

/**
 * Test the request-reply use case.
 */
@RunWith(VertxUnitRunner.class)
public class RequestReplyTest extends ArtemisTestBase {

  private Vertx vertx;

  @Before
  public void before() {
    vertx = Vertx.vertx();
  }

  @After
  public void after(TestContext tc) {
    vertx.close(tc.asyncAssertSuccess());
  }

  private Future<Void> prepareReceiver(TestContext context, AmqpConnection connection, String address) {
    Future<Void> future = Future.future();
    connection.createReceiver(address, msg -> {
      context.assertEquals("what's your name?", msg.bodyAsString());
      context.assertTrue(msg.replyTo() != null);
      // How do we name this createSender method where the address is not set?
      connection.createAnonymousSender(sender ->
        sender.result().send(AmqpMessage.create().address(msg.replyTo()).withBody("my name is Neo").build()));
    }, d -> future.handle(d.mapEmpty()));
    return future;
  }

  private Future<AmqpReceiver> prepareReplyReceiver(TestContext context, AmqpConnection connection, Async done) {
    Future<AmqpReceiver> future = Future.future();
    connection.createDynamicReceiver(rec -> {
      context.assertTrue(rec.succeeded());
      AmqpReceiver receiver = rec.result();
      context.assertNotNull(receiver.address());
      receiver.handler(message -> {
        context.assertEquals(message.bodyAsString(), "my name is Neo");
        done.complete();
      });
      future.complete(receiver);
    });
    return future;
  }

  private Future<Void> getSenderAndSendInitialMessage(TestContext context, AmqpConnection connection, String address,
    String replyAddress) {
    Future<Void> future = Future.future();
    connection.createSender(address, ar -> {
      context.assertTrue(ar.succeeded());
      ar.result().sendWithAck(
        AmqpMessage.create().address(address)
          .replyTo(replyAddress)
          .withBody("what's your name?").build(),
        ack -> future.handle(ack.mapEmpty())
      );
    });
    return future;
  }

  @Test
  public void testRequestReply(TestContext context) {
    String queue = UUID.randomUUID().toString();
    Async done = context.async();
    client = AmqpClient.create(vertx, new AmqpClientOptions()
      .setHost(host).setPort(port).setPassword(password).setUsername(username))
      .connect(conn -> {
        context.assertTrue(conn.succeeded());

        prepareReceiver(context, conn.result(), queue)
          .compose(x -> prepareReplyReceiver(context, conn.result(), done))
          .compose(dr -> getSenderAndSendInitialMessage(context, conn.result(), queue, dr.address()));
      });
  }

}
