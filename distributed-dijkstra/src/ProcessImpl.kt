package dijkstra

import dijkstra.messages.*
import dijkstra.system.environment.Environment

class ProcessImpl(private val environment: Environment) : Process {
    private var distance: Long? = null
    private var sourceId: Int? = null
    private var childCount: Int = 0
    private var balance: Int = 0

    private fun updateNeighbors() {
        balance += environment.neighbours.size
        for (child in environment.neighbours) {
            environment.send(child.key, MessageUpdateDistance(distance!! + child.value))
        }
        checkProcessColor()
    }

    private fun checkProcessColor() {
        if (childCount == 0 && balance == 0) {
            if (sourceId == null) {
                environment.finishExecution()
            } else {
                environment.send(sourceId!!, MessageGoodBayDaddy)
                sourceId = null
            }
        }
    }

    override fun onMessage(srcId: Int, message: Message) {
        when (message) {
            is MessageUpdateDistance -> {
                if (distance == null || distance!! > message.newDistance) {
                    if (sourceId != null) {
                        environment.send(srcId, MessageImNotYourSon)
                    } else {
                        sourceId = srcId
                        environment.send(srcId, MessageHelloDaddy)
                    }

                    distance = message.newDistance
                    updateNeighbors()

                } else {
                    environment.send(srcId, MessageImNotYourSon)
                }
            }
            is MessageImNotYourSon -> {
                --balance
                checkProcessColor()
            }
            is MessageHelloDaddy -> {
                ++childCount
                --balance
            }
            is MessageGoodBayDaddy -> {
                --childCount
                checkProcessColor()
            }
        }
    }

    override fun getDistance(): Long? {
        return distance
    }

    override fun startComputation() {
        distance = 0
        updateNeighbors()
    }

}