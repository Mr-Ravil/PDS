package dijkstra.messages

sealed class Message

data class MessageUpdateDistance(val newDistance: Long) : Message()

object MessageImNotYourSon : Message()

object MessageHelloDaddy : Message()

object MessageGoodBayDaddy : Message()
