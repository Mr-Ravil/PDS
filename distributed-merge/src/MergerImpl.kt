import system.MergerEnvironment

class MergerImpl<T : Comparable<T>>(
        private val mergerEnvironment: MergerEnvironment<T>,
        prevStepBatches: Map<Int, List<T>>?
) : Merger<T> {
    private val batches: HashMap<Int, List<T>> = HashMap()

    init {
        if (prevStepBatches != null) {
            batches.putAll(prevStepBatches)
        } else {
            for (i in 0 until mergerEnvironment.dataHoldersCount) {
                batches[i] = mergerEnvironment.requestBatch(i)
                if (batches[i]!!.isEmpty()) {
                    batches.remove(i)
                }
            }
        }
    }

    override fun mergeStep(): T? {
        var minimal: T? = null
        var batchKey: Int = -1
        for (batch in batches) {
            if (minimal == null || batch.value.first() < minimal) {
                minimal = batch.value.first()
                batchKey = batch.key
            }
        }
        if (minimal != null) {
            if (batches[batchKey]!!.size > 1) {
                batches[batchKey] = batches[batchKey]!!.subList(1, batches[batchKey]!!.size)
            } else {
                batches[batchKey] = mergerEnvironment.requestBatch(batchKey)
                if (batches[batchKey]!!.isEmpty()) {
                    batches.remove(batchKey)
                }
            }
        }
        return minimal
    }

    override fun getRemainingBatches(): Map<Int, List<T>> {
        return batches
    }
}