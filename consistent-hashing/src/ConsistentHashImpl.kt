class ConsistentHashImpl<K> : ConsistentHash<K> {
    private var circle: ArrayList<Node> = ArrayList()

    override fun getShardByKey(key: K): Shard {
        val hashCode = key.hashCode()
        var l = 0
        var r = circle.size
        while (l + 1 < r) {
            val m = l + (r - l) / 2
            if (hashCode < circle[m].hashCode) {
                r = m
            } else {
                l = mx
            }
        }
        if (hashCode > circle[l].hashCode)
            ++l
        return circle[l % circle.size].shard
    }

    override fun addShard(newShard: Shard, vnodeHashes: Set<Int>): Map<Shard, Set<HashRange>> {
        val newCircle: ArrayList<Node> = ArrayList()
        val newHashes: ArrayList<Int> = ArrayList(vnodeHashes)
        newHashes.sortWith { a, b -> a.compareTo(b) }

        if (circle.isEmpty()) {
            for (hashCode in newHashes) {
                circle.add(Node(newShard, hashCode))
            }
            return emptyMap()
        }

        var i = 0 // old circle iterator
        var j = 0 // new elements to circle iterator
        var rangeFrom = circle[circle.size - 1].hashCode + 1
        val needToMove: HashMap<Shard, HashSet<HashRange>> = HashMap()

        while (j < newHashes.size) {
            while (i < circle.size && circle[i].hashCode < newHashes[j])
                newCircle.add(circle[i++])

            if (newCircle.isNotEmpty()) {
                rangeFrom = newCircle[newCircle.size - 1].hashCode + 1
            }

            if (i < circle.size) {
                while (j < newHashes.size && newHashes[j] < circle[i].hashCode)
                    newCircle.add(Node(newShard, newHashes[j++]))

                val oldShard: Shard = circle[i].shard
                if (needToMove.containsKey(oldShard)) {
                    needToMove[oldShard]!!.add(HashRange(rangeFrom, newHashes[j - 1]))
                } else {
                    needToMove[oldShard] = HashSet(setOf(HashRange(rangeFrom, newHashes[j - 1])))
                }
            } else {
                while (j < newHashes.size)
                    newCircle.add(Node(newShard, newHashes[j++]))

                if (newCircle[0].shard != newShard) {
                    val oldShard: Shard = circle[0].shard
                    if (needToMove.containsKey(oldShard)) {
                        needToMove[oldShard]!!.add(HashRange(rangeFrom, newHashes[j - 1]))
                    } else {
                        needToMove[oldShard] = HashSet(setOf(HashRange(rangeFrom, newHashes[j - 1])))
                    }
                }
            }

        }
        while (i < circle.size)
            newCircle.add(circle[i++])

        circle = newCircle
        return needToMove
    }

    override fun removeShard(shard: Shard): Map<Shard, Set<HashRange>> {
        val newCircle: ArrayList<Node> = ArrayList()
        val needToMove: HashMap<Shard, HashSet<HashRange>> = HashMap()

        var lastIt = circle.size - 1
        while (circle[lastIt].shard == shard) // we shouldn't check j >= 0, because such j must exist
            lastIt--

        var rangeFrom = circle[lastIt].hashCode + 1
        var i = 0
        while (i < circle.size) {
            if (circle[i].shard != shard) {
                newCircle.add(circle[i++])
                continue
            }

            if (newCircle.isNotEmpty()) {
                rangeFrom = newCircle[newCircle.size - 1].hashCode + 1
            }

            if (i < lastIt) {
                while (circle[i].shard == shard) ++i

                if (needToMove.containsKey(circle[i].shard)) {
                    needToMove[circle[i].shard]!!.add(HashRange(rangeFrom, circle[i - 1].hashCode))
                } else {
                    needToMove[circle[i].shard] = HashSet(setOf(HashRange(rangeFrom, circle[i - 1].hashCode)))
                }
            } else {
                if (circle[0].shard != shard) {
                    if (needToMove.containsKey(circle[0].shard)) {
                        needToMove[circle[0].shard]!!.add(HashRange(rangeFrom, circle[circle.size - 1].hashCode))
                    } else {
                        needToMove[circle[0].shard] =
                            HashSet(setOf(HashRange(rangeFrom, circle[circle.size - 1].hashCode)))
                    }
                }
                break
            }
        }

        circle = newCircle
        return needToMove
    }

    class Node(val shard: Shard, val hashCode: Int)
}
