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

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonServer;
import io.vertx.proton.ProtonServerOptions;

import java.util.concurrent.ExecutionException;

public class MockServer {
  private ProtonServer server;

  // Toggle to (re)use a fixed port, e.g for capture.
  private int bindPort = 0;
  private boolean reuseAddress = false;

  public MockServer(Vertx vertx, Handler<ProtonConnection> connectionHandler)
    throws ExecutionException, InterruptedException {
    this(vertx, connectionHandler, null);
  }

  public MockServer(Vertx vertx, Handler<ProtonConnection> connectionHandler, ProtonServerOptions protonServerOptions)
    throws ExecutionException, InterruptedException {
    if (protonServerOptions == null) {
      protonServerOptions = new ProtonServerOptions();
    }

    protonServerOptions.setReuseAddress(reuseAddress);
    server = ProtonServer.create(vertx, protonServerOptions);
    server.connectHandler(connectionHandler);

    FutureHandler<ProtonServer, AsyncResult<ProtonServer>> handler = FutureHandler.asyncResult();
    server.listen(bindPort, handler);
    handler.get();
  }

  public int actualPort() {
    return server.actualPort();
  }

  public void close() {
    server.close();
  }

  ProtonServer getProtonServer() {
    return server;
  }
}
