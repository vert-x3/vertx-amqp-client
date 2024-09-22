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
module io.vertx.amqp.client {

  requires static io.vertx.codegen.api;
  requires static io.vertx.codegen.json;
  requires static io.vertx.docgen;

  requires io.vertx.core;
  requires io.vertx.core.logging;
  requires io.vertx.proton;
  requires java.sql;
  requires org.apache.qpid.proton.j;
  requires static org.reactivestreams;

  exports io.vertx.amqp;
  exports io.vertx.amqp.impl to io.vertx.amqp.client.tests;

}
