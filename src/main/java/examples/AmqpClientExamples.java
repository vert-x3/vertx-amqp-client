/*
 * Copyright (c) 2018-2019 The original author or authors
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *        The Eclipse Public License is available at
 *        http://www.eclipse.org/legal/epl-v10.html
 *
 *        The Apache License v2.0 is available at
 *        http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */
package examples;

import io.vertx.amqp.*;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

public class AmqpClientExamples {

  public void creation(Vertx vertx) {
    AmqpClientOptions options = new AmqpClientOptions()
      .setHost("localhost")
      .setPort(5672)
      .setUsername("user")
      .setPassword("secret");
    // Create a client using its own internal Vert.x instance.
    AmqpClient client1 = AmqpClient.create(options);

    // USe an explicit Vert.x instance.
    AmqpClient client2 = AmqpClient.create(vertx, options);
  }

  public void connect(AmqpClient client) {
    client
      .connect()
      .onComplete(ar -> {
      if (ar.failed()) {
        System.out.println("Unable to connect to the broker");
      } else {
        System.out.println("Connection succeeded");
        AmqpConnection connection = ar.result();
      }
    });
  }

  public void receiver1(AmqpConnection connection) {
    connection
      .createReceiver("my-queue")
      .onComplete(
      done -> {
        if (done.failed()) {
          System.out.println("Unable to create receiver");
        } else {
          AmqpReceiver receiver = done.result();
          receiver.handler(msg -> {
            // called on every received messages
            System.out.println("Received " + msg.bodyAsString());
          });
        }
      }
    );
  }

  public void receiverFromClient(AmqpClient client) {
    client
      .createReceiver("my-queue")
      .onComplete(
      done -> {
        if (done.failed()) {
          System.out.println("Unable to create receiver");
        } else {
          AmqpReceiver receiver = done.result();
          receiver.handler(msg -> {
            // called on every received messages
            System.out.println("Received " + msg.bodyAsString());
          });
        }
      }
    );
  }

  public void senderFromClient(AmqpClient client) {
    client
      .createSender("my-queue")
      .onComplete(maybeSender -> {
      //...
    });
  }

  public void receiver2(AmqpConnection connection) {
    connection
      .createReceiver("my-queue")
      .onComplete(
      done -> {
        if (done.failed()) {
          System.out.println("Unable to create receiver");
        } else {
          AmqpReceiver receiver = done.result();
          receiver
            .exceptionHandler(t -> {
              // Error thrown.
            })
            .handler(msg -> {
              // Attach the message handler
            });
        }
      }
    );
  }

  public void sender(AmqpConnection connection) {
    connection
      .createSender("my-queue")
      .onComplete(done -> {
      if (done.failed()) {
        System.out.println("Unable to create a sender");
      } else {
        AmqpSender result = done.result();
      }
    });
  }

  public void messages() {
    // Retrieve a builder
    AmqpMessageBuilder builder = AmqpMessage.create();

    // Very simple message
    AmqpMessage m1 = builder.withBody("hello").build();

    // Message overriding the destination
    AmqpMessage m2 = builder.withBody("hello").address("another-queue").build();

    // Message with a JSON object as body, metadata and TTL
    AmqpMessage m3 = builder
      .withJsonObjectAsBody(new JsonObject().put("message", "hello"))
      .subject("subject")
      .ttl(10000)
      .applicationProperties(new JsonObject().put("prop1", "value1"))
      .build();
  }

  public void send(AmqpSender sender) {
    sender.send(AmqpMessage.create().withBody("hello").build());
  }

  public void sendWithAck(AmqpSender sender) {
    sender
      .sendWithAck(AmqpMessage.create().withBody("hello").build())
      .onComplete(acked -> {
      if (acked.succeeded()) {
        System.out.println("Message accepted");
      } else {
        System.out.println("Message not accepted");
      }
    });
  }

  public void requestReply(AmqpConnection connection) {
    // On the receiver side (receiving the initial request and replying)
    connection
      .createAnonymousSender()
      .onComplete(responseSender -> {
      // You got an anonymous sender, used to send the reply
      // Now register the main receiver:
      connection
        .createReceiver("my-queue")
        .onComplete(done -> {
        if (done.failed()) {
          System.out.println("Unable to create receiver");
        } else {
          AmqpReceiver receiver = done.result();
          receiver.handler(msg -> {
            // You got the message, let's reply.
            responseSender.result().send(AmqpMessage.create()
              .address(msg.replyTo())
              .correlationId(msg.id()) // send the message id as correlation id
              .withBody("my response to your request")
              .build()
            );
          });
        }
      });
    });

    // On the sender side (sending the initial request and expecting a reply)
    connection
      .createDynamicReceiver()
      .onComplete(replyReceiver -> {
      // We got a receiver, the address is provided by the broker
      String replyToAddress = replyReceiver.result().address();

      // Attach the handler receiving the reply
      replyReceiver.result().handler(msg -> {
        System.out.println("Got the reply! " + msg.bodyAsString());
      });

      // Create a sender and send the message:
      connection
        .createSender("my-queue")
        .onComplete(sender -> {
        sender.result().send(AmqpMessage.create()
          .replyTo(replyToAddress)
          .id("my-message-id")
          .withBody("This is my request").build());
      });
    });
  }
}
