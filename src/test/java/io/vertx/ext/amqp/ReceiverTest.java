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
package io.vertx.ext.amqp;

import org.junit.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class ReceiverTest extends ArtemisTestBase {

  @Test
  public void testReception() {
    AtomicInteger count = new AtomicInteger();
    String queue = UUID.randomUUID().toString();
    List<String> list = new CopyOnWriteArrayList<>();
    client = AmqpClient.create(new AmqpClientOptions()
      .setHost(host)
      .setPort(port)
      .setUsername(username)
      .setPassword(password)
    ).connect(connection -> {
        connection.result().receiver(queue, message -> list.add(message.bodyAsString()),
          done ->
            CompletableFuture.runAsync(() -> {
              usage.produceStrings(queue, 10, null,
                () -> Integer.toString(count.getAndIncrement()));
            })
        );
      }
    );

    await().until(() -> list.size() == 10);
    assertThat(list).containsExactly("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
  }


}
