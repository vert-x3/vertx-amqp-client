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
package io.vertx.amqp;

import io.vertx.core.Vertx;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;

/**
 * Test that messages are not lost when the receiver disconnects and reconnects.
 */
public class DurabilityTest extends ArtemisTestBase {

  private Vertx vertx;

  private AmqpClient client1, client2;

  @Before
  public void init() {
    vertx = Vertx.vertx();
  }

  @After
  public void destroy() {
    AtomicBoolean c1 = new AtomicBoolean();
    AtomicBoolean c2 = new AtomicBoolean();
    client1.close(x -> c1.set(true));
    client2.close(x -> c2.set(true));
    await().untilAtomic(c1, is(true));
    await().untilAtomic(c2, is(true));
    vertx.close();
  }

  @Test
  public void test() {
    AtomicBoolean c1 = new AtomicBoolean();
    AtomicBoolean c2 = new AtomicBoolean();
    AmqpClientOptions options = new AmqpClientOptions()
      .setHost(host)
      .setPort(port)
      .setUsername(username)
      .setPassword(password);

    client1 = AmqpClient.create(options
    ).connect(
      ar -> c1.set(ar.succeeded())
    );

    client2 = AmqpClient.create(options
    ).connect(
      ar -> c2.set(ar.succeeded())
    );

    await().untilAtomic(c1, is(true));
    await().untilAtomic(c2, is(true));

    c1.set(false);
    c2.set(false);
    List<String> list = new CopyOnWriteArrayList<>();
    client1.createReceiver("my-address",
      new AmqpReceiverOptions().setDurable(true),
      msg -> list.add(msg.bodyAsString()),
      done -> c1.set(done.succeeded()));
    await().untilAtomic(c1, is(true));

    AtomicReference<AmqpSender> sender = new AtomicReference<>();
    client2.createSender("my-address",
      done -> sender.set(done.result()));
    await().untilAtomic(sender, is(not(nullValue())));

    sender.get().send(AmqpMessage.create().withBody("a").durable(true).build());
    sender.get().send(AmqpMessage.create().withBody("b").durable(true).build());

    await().until(() -> list.contains("a") && list.contains("b"));

    client1.close(v -> c2.set(true));
    await().untilAtomic(c2, is(true));

    sender.get().send(AmqpMessage.create().withBody("c").durable(true).build());
    sender.get().send(AmqpMessage.create().withBody("d").durable(true).build());

    c1.set(false);
    client1 = AmqpClient.create(options
    ).connect(x ->
      client1.createReceiver("my-address",
        new AmqpReceiverOptions().setDurable(true),
        msg -> list.add(msg.bodyAsString()),
        done -> {
          if (done.failed()) {
            done.cause().printStackTrace();
          }
          c1.set(done.succeeded());
        })
    );
    await().untilAtomic(c1, is(true));

    sender.get().send(AmqpMessage.create().withBody("e").durable(true).build());
    sender.get().send(AmqpMessage.create().withBody("f").durable(true).build());

    await().until(() -> list.contains("e") && list.contains("f"));

    sender.get().send(AmqpMessage.create().withBody("g").durable(true).build());
    await().until(() -> list.contains("g"));
  }

}
