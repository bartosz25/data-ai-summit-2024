package com.waitingforcode.test

import java.util.TimeZone

trait ExecutionContext {

  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

}
