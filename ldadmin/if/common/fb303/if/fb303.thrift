/*
 * fb303.thrift
 *
 * Copyright (c) Facebook, Inc. and its affiliates.
 * Distributed under the Thrift Software License
 *
 * See accompanying file LICENSE or visit the Thrift site at:
 * http://developers.facebook.com/thrift/
 *
 *
 * Definition of common Facebook data types and status reporting mechanisms
 * common to all Facebook services. In some cases, these methods are
 * provided in the base implementation, and in other cases they simply define
 * methods that inheriting applications should implement (i.e. status report)
 *
 * @author Mark Slee <mcslee@facebook.com>
 */
namespace java com.facebook.fbcode.fb303
namespace java.swift com.facebook.swift.fb303
namespace cpp facebook.fb303
namespace py.asyncio fb303_asyncio.fb303
namespace perl fb303
namespace php fb303
namespace node_module fb303
namespace go common.fb303.if.fb303

/**
 * Common status reporting mechanism across all services
 */
enum fb_status {
  DEAD = 0,
  STARTING = 1,
  ALIVE = 2,
  STOPPING = 3,
  STOPPED = 4,
  WARNING = 5,
}

/**
 * Standard base service
 *
 */
service FacebookService {
  /**
   * Returns the version of the service
   */
  string getVersion(),

  /**
   * Gets the status of this service
   */
  fb_status getStatus() (priority='IMPORTANT'),

  /**
   * Returns the unix time that the server has been running since
   */
  i64 aliveSince() (priority = 'IMPORTANT'),

  /**
   * Returns the pid of the process
   */
  i64 getPid(),
}
