package model

case class Event (
  eventType: String,
  count: Int,
  name: String
) extends Serializable
