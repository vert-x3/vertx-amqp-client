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

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.amqp.*;

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
    client.connect(ar -> {
      if (ar.failed()) {
        System.out.println("Unable to connect to the broker");
      } else {
        System.out.println("Connection succeeded");
        AmqpConnection connection = ar.result();
      }
    });
  }

  public void receiver1(AmqpConnection connection) {
    connection.receiver("my-queue",
      msg -> {
        // called on every received messages
        System.out.println("Received " + msg.bodyAsString());
      },
      done -> {
        if (done.failed()) {
          System.out.println("Unable to create receiver");
        } else {
          AmqpReceiver receiver = done.result();
        }
      }
    );
  }

  public void receiver2(AmqpConnection connection) {
    connection.receiver("my-queue",
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
    connection.sender("my-queue", done -> {
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

  public void sendWithAddress(AmqpSender sender) {
    sender.send("another-queue", AmqpMessage.create().withBody("hello").build());
  }

  public void sendWithAck(AmqpSender sender) {
    sender.send(AmqpMessage.create().withBody("hello").build(), acked -> {
      if (acked.succeeded()) {
        System.out.println("Message accepted");
      } else {
        System.out.println("Message not accepted");
      }
    });
  }

  public void sendWithReply(AmqpSender sender) {
    sender.send(AmqpMessage.create().withBody("Hello, how you you?").build(), reply -> {
      if (reply.succeeded()) {
        AmqpMessage response = reply.result();
      } else {
        System.out.println("No response before timeout");
      }
    });
  }

  public void receiveAndReply(AmqpConnection connection) {
    connection.receiver("my-queue",
      msg -> {
        msg.reply(AmqpMessage.create().withBody("I'm doing great, thanks!").build());
      }, done -> {

      });
  }



}
