package mutex


/**
 * Distributed mutual exclusion implementation.
 * All functions are called from the single main thread.
 *
 * @author Ravil Galiev
 */
class ProcessImpl(private val env: Environment) : Process {
    private var forkState = Array(env.nProcesses) { i -> if (env.processId > i + 1) Fork.TAKEN else Fork.FILTHY }
    private var forkReq = BooleanArray(env.nProcesses) { false }
    private var inCS = false // are we in critical section?
    private var needForkCount = env.processId - 1

    override fun onMessage(srcId: Int, message: Message) {
        /* todo: write implementation here */
        val srcIndex = srcId - 1
        message.parse {
            when (readEnum<MsgType>()) {
                MsgType.REQ -> {
                    if (!inCS && forkState[srcIndex] != Fork.CLEAN) {
                        needForkCount++
                        forkState[srcIndex] = Fork.TAKEN
                        env.send(srcId) {
                            writeEnum(MsgType.OK)
                        }
                    } else {
                        forkReq[srcIndex] = true
                    }
                }
                MsgType.OK -> {
                    needForkCount--
                    forkState[srcIndex] = Fork.CLEAN
                    onLockRequest()
                }
            }
        }
    }

    override fun onLockRequest() {
        /* todo: write implementation here */
        inCS = true
        if (needForkCount == 0) {
            env.locked()
            return
        }
        inCS = false
        for (i in 1..env.nProcesses) {
            // loop (edge from us to us) is always Filthy
            if (forkState[i - 1] == Fork.TAKEN) {
                env.send(i) {
                    writeEnum(MsgType.REQ)
                }
                forkState[i - 1] = Fork.REQ
            }
        }
    }

    override fun onUnlockRequest() {
        /* todo: write implementation here */
        env.unlocked()
        for (i in 0 until env.nProcesses) {
            if (forkReq[i]) {
                env.send(i + 1) {
                    writeEnum(MsgType.OK)
                }
                needForkCount++
                forkState[i] = Fork.TAKEN
                forkReq[i] = false
            } else {
                forkState[i] = Fork.FILTHY
            }
        }
        inCS = false
    }

    internal enum class Fork {
        CLEAN, FILTHY, TAKEN, REQ
    }

    internal enum class MsgType {
        REQ, OK
    }

}