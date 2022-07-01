import system.DataHolderEnvironment

class DataHolderImpl<T : Comparable<T>>(
        private val keys: List<T>,
        private val dataHolderEnvironment: DataHolderEnvironment
) : DataHolder<T> {
    private var lastCheckpoint = 0
    private var currentPoint = 0

    override fun checkpoint() {
        lastCheckpoint = currentPoint
    }

    override fun rollBack() {
        currentPoint = lastCheckpoint
    }

    override fun getBatch(): List<T> {
        val returnedKeys = keys.filterIndexed { i, _ ->
            i >= currentPoint && i < currentPoint + dataHolderEnvironment.batchSize
        }
        currentPoint += dataHolderEnvironment.batchSize
        return returnedKeys
    }
}