package com.waitingforcode.builders

import difflicious.Differ

final case class DeviceUnderTest(`type`: String, version: String, full_name: String)

object DeviceUnderTest {

  def build(device_type: String = "pc", version: String = "1.2.3",
            full_name: String = "PC v1.2.3 pro"): DeviceUnderTest = {
    DeviceUnderTest(`type` = device_type, version = version, full_name = full_name)
  }

  def commonDevices(): Seq[DeviceUnderTest] = {
    Seq(
      build(device_type = "pc", version = "1.2.3", full_name="PC version 1.2.3-beta"),
      build(device_type = "pc", version = "4.5.6", full_name="PC version 4.5.6-beta")
    )
  }
}