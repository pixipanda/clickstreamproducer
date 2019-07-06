package com.pixipanda.producer



abstract class Producer {

  def publish(event: Object)
}