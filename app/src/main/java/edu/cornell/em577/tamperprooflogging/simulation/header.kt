package edu.cornell.em577.tamperprooflogging.simulation

import com.vegvisir.data.ProtocolMessageProto.ProtocolMessage
import edu.cornell.em577.tamperprooflogging.data.model.*
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.math.roundToInt

typealias argsType = Array<out Any?>?

class Message {
    enum class messageType {
        PROTOMESSAGE,
        BLOCKS,
    }

    private var protoMessage: ProtocolMessage? = null
    private var blocks: List<SignedBlock>? = null
    constructor(m: ProtocolMessage) {
        //type = messageType.PROTOMESSAGE
        protoMessage = m
    }

    constructor(b: List<SignedBlock>) {
        //type = messageType.BLOCKS
        blocks = b
    }

    val type: messageType by lazy {
        if (protoMessage != null)
            messageType.PROTOMESSAGE
        else if (blocks != null)
            messageType.BLOCKS
        else throw RuntimeException("Undefined message")
    }

    val size: Int by lazy {
        when (type) {
            messageType.PROTOMESSAGE -> protoMessage!!.serializedSize
            messageType.BLOCKS -> {
                var sum = 0
                blocks!!.forEach { sum += it.toProto().serializedSize }
                sum
            }
        }
    }

    fun asProtoMessage(): ProtocolMessage {
        return protoMessage!!
    }

    fun asBlocks(): List<SignedBlock> {
        return blocks!!
    }

}

class networkChannel constructor(
        private val sim: Simulator,
        private val id: Int,
        private val port: Int,
        private val delay: Double,
        private val bandwidth: Double,
        private val name: String = "undefined"
) {
    companion object {
        const val streamInterval: Double = 5.toDouble()
    }

    private var closed = false
    private val inputBuff = LinkedList<Message>()
    private val waitingCallbacks = LinkedList<Pair<(Message, argsType) -> Unit, argsType>>()
    private val outputBuff = LinkedList<Pair<Message, Pair<(argsType) -> Unit, argsType>?>>()
    private var waitingOnNewOutput = true
    private val sliceSize = streamInterval * bandwidth

    fun info(): Pair<Int, Int> {
        return Pair(id, port)
    }

    fun send(m: Message) {
        if (closed) {
            sim.handleError("try to send after closed on network channel {name: $name, port:($id, $port)}")
            return
        }
        outputBuff.add(Pair(m, null))
        if (waitingOnNewOutput)
            handleOutput(null)
    }

    fun onReceive(callback: (Message, argsType) -> Unit, args: argsType) {
        if (closed) {
            sim.handleError("try to receive after closed on network channel {name: $name, port:($id, $port)}")
            return
        }
        if (inputBuff.isNotEmpty())
            callback(inputBuff.poll(), args)
        else
            waitingCallbacks.addLast(Pair(callback, args))
    }

    // Only used for simulator
    // Please use onReceive
    fun receive(m: Message) {
        if (waitingCallbacks.isNotEmpty()) {
            val (callback, args) = waitingCallbacks.poll()
            callback(m, args)
        } else
            inputBuff.addLast(m)
    }

    fun syncStreamWrapper(blocks: ArrayList<SignedBlock>, callback: (argsType) -> Unit, extra: argsType) {
        synchronousStream(arrayOf(blocks, callback, extra))
    }

    private fun synchronousStream(args: argsType) {
        args!!
        val blocks = args[0] as ArrayList<SignedBlock>
        val callback = args[1] as (argsType) -> Unit
        val extra = args[2] as argsType
        if (closed) {
            sim.handleError("try to stream after closed on network channel {name: $name, port:($id, $port)}")
            callback(extra)
        } else {
            var sizeSoFar = 0
            val tempList = ArrayList<SignedBlock>()
            while (blocks.isNotEmpty()) {
                val b = blocks.removeAt(0)
                tempList.add(b)
                sizeSoFar += b.toProto().serializedSize
                if (sizeSoFar >= sliceSize)
                    break
            }
            val temp = Pair(Message(tempList),
                    if (blocks.isEmpty())
                        Pair(callback, extra)
                    else
                        Pair(::synchronousStream, arrayOf(blocks, callback, extra))
            )
            outputBuff.add(temp)
            if (waitingOnNewOutput)
                handleOutput(null)
        }

    }

    fun close() {
        if (!closed) {
            val information = HashMap<String, Any>()
            information["id"] = id
            information["port"] = port
            sim.addEvent(Event(Event.eventType.CONNECTION_CLOSED, sim.time(), information, "Port ($id,$port) is closed"))
            closed = true
        } else
            sim.handleError("try to close after closed on channel {name: $name, port:($id, $port)}")
    }

    private fun handleOutput(whoCare: argsType) {
        if (outputBuff.isEmpty()) {
            waitingOnNewOutput = true
            return
        } else {
            waitingOnNewOutput = false
            val (m, extra) = outputBuff.poll()
            val sendTime = m.size / bandwidth
            val triggerTime = sim.time() + delay + sendTime
            val information = HashMap<String, Any>()
            information["id"] = id
            information["port"] = 1 - port
            information["message"] = m
            sim.addEvent(Event(Event.eventType.RECEIVED_THINGS, triggerTime, information))
            sim.timer(sendTime, ::handleOutput, null)
            if (extra != null) {
                val (callback, args) = extra
                callback(args)
            }
        }
    }

    // uglyyyyyy
    // I hate object-oriented programming
    fun delay(): Double {
        return delay
    }

    fun bandwidth():Double{
        return bandwidth
    }
}

class Channel<T : Any> constructor(
        private val sim: Simulator,
        private val id: Int,
        private val port: Int,
        private val name: String = "undefined"
) {
    companion object {
        const val delay = 1.toDouble()
    }

    private var closed = false
    private var remoteClosed = false
    private val inputBuff = LinkedList<T>()
    private val waitingCallbacks = LinkedList<Pair<(T, argsType) -> Unit, argsType>>()
    private var onClose: Pair<(argsType) -> Unit, argsType> = Pair({ _: argsType -> }, null)

    fun info(): Pair<Int, Int> {
        return Pair(id, port)
    }

    fun send(t: T,description: String="") {
        if (closed) {
            sim.handleError("try to send after closed on channel {name: $name, port:($id, $port)}")
            return
        }
        val triggerTime = sim.time() + delay
        val information = HashMap<String, Any>()
        information["id"] = id
        information["port"] = 1 - port
        information["message"] = t
        sim.addEvent(Event(Event.eventType.RECEIVED_THINGS, triggerTime, information,description))
    }

    fun onReceive(callback: (T, argsType) -> Unit, args: argsType) {
        if (closed) {
            sim.handleError("try to receive after closed on channel {name: $name, port:($id, $port)}")
            return
        }
        if (inputBuff.isNotEmpty())
            callback(inputBuff.poll(), args)
        else
            if (remoteClosed) {
                val (closeCB, extra) = onClose
                closeCB(extra)
            } else
                waitingCallbacks.addLast(Pair(callback, args))
    }

    // Only used for simulator
    // Please use OnReceive
    fun receive(t: T) {
        if (waitingCallbacks.isNotEmpty()) {
            val (callback, args) = waitingCallbacks.poll()
            callback(t, args)
        } else
            inputBuff.addLast(t)
    }


    fun setCloseCB(callback: (argsType) -> Unit, args: argsType) {
        onClose = Pair(callback, args)
    }

    // Only used for simulator
    fun setRemoteClose() {
        if (!remoteClosed) {
            remoteClosed = true
            while (waitingCallbacks.isNotEmpty()) {
                waitingCallbacks.poll()
                val (closeCB, extra) = onClose
                closeCB(extra)
            }
        }
    }

    fun close() {
        if (!closed) {
            val information = HashMap<String, Any>()
            information["id"] = id
            information["port"] = port
            sim.addEvent(Event(Event.eventType.CHANNEL_CLOSED, sim.time(), information, "Port ($id,$port) is closed"))
            closed = true
        } else
            sim.handleError("try to close after closed on channel {name: $name, port:($id, $port)}")
    }
}

data class Event(
        val type: eventType,
        val time: Double,
        val information: HashMap<String, Any>,
        val description: String = ""
) {
    enum class eventType {
        CONNECTION_BUILD,
        CONNECTION_LOST,
        USER_ADD_TRANSACTION,
        RECEIVED_THINGS,
        TIMER_EXPIRED,
        CHANNEL_CLOSED,
        CONNECTION_CLOSED
    }

    companion object {
        fun createConnection(id1: String, id2: String, time: Double, delay: Double, bandwidth: Double, description: String = ""): Event {
            val info = HashMap<String, Any>()
            info["endpointId1"] = id1
            info["endpointId2"] = id2
            info["delay"] = delay
            info["bandwidth"] = bandwidth

            return Event(eventType.CONNECTION_BUILD, time, info, description)
        }

        fun cutConnection(id1: String, id2: String, time: Double, description: String = ""): Event {
            val info = HashMap<String, Any>()
            info["endpointId1"] = id1
            info["endpointId2"] = id2
            return Event(eventType.CONNECTION_LOST, time, info, description)
        }

        fun addTransaction(id: String, time: Double, transaction: Transaction, description: String = ""): Event {
            val info = HashMap<String, Any>()
            info["endpointId"] = id
            info["transaction"] = transaction
            return Event(eventType.USER_ADD_TRANSACTION, time, info, description)
        }
    }
}

class Simulator() {

    private val eventQ = PriorityQueue<Pair<Event, Int>>({ p0: Pair<Event, Int>?, p1: Pair<Event, Int>? ->
        if (p0!!.first.time != p1!!.first.time)
            if (p0.first.time < p1.first.time) -1
            else 1
        else
            p0.second - p1.second
    })
    private var time: Double = 0.toDouble()
    private var idSofar: Int = 0
    private val port2Channel = HashMap<Pair<Int, Int>, Channel<*>>()
    private val port2networkChannel = HashMap<Pair<Int, Int>, networkChannel>()
    private val timerCallbacks = HashMap<Int, Pair<(argsType) -> Unit, argsType>>()
    private val endpointId2Protocols = HashMap<String, Protocol>()
    private val port2EndpointId = HashMap<Pair<Int, Int>, String>()
    private var endTime: Double = 0.toDouble()

    enum class debugLevel {
        throwExecption,
        printInfo,
        noAction
    }

    private var dbgLvl: debugLevel = debugLevel.noAction

    fun setEndTime(time: Double) {
        if (endTime < time) endTime = time
    }

    fun setDbgLvl(lvl: debugLevel) {
        dbgLvl = lvl
    }

    fun handleError(str: String) {
        when (dbgLvl) {
            Simulator.debugLevel.printInfo -> println(str)
            Simulator.debugLevel.throwExecption -> throw RuntimeException(str)
            Simulator.debugLevel.noAction -> return
        }
    }

    fun addEvent(e: Event) {
        eventQ.add(Pair(e, nextID()))
    }

    private fun <T : Any> wrappedReceive(m: Any, ch: Channel<T>) {
        ch.receive(m as T)
    }

    fun run() {
        while (eventQ.isNotEmpty()) {
            val e = eventQ.poll().first
            when (dbgLvl) {
                debugLevel.printInfo, debugLevel.throwExecption -> println(e)
                debugLevel.noAction -> {
                }
            }
            time = e.time
            when (e.type) {
                Event.eventType.RECEIVED_THINGS -> {
                    val p = Pair(e.information["id"]!! as Int, e.information["port"]!! as Int)
                    if (port2networkChannel.containsKey(p))
                        port2networkChannel[p]!!.receive(e.information["message"]!! as Message)
                    else if (port2Channel.containsKey(p))
                        wrappedReceive(e.information["message"]!!, port2Channel[p]!!)
                }
                Event.eventType.CHANNEL_CLOSED -> {
                    val p = Pair(e.information["id"]!! as Int, 1 - e.information["port"]!! as Int)
                    timer(Channel.delay, { _: argsType -> port2Channel[p]!!.setRemoteClose() }, null)
                }
                Event.eventType.TIMER_EXPIRED -> {
                    val id = e.information["id"]!! as Int
                    val (callback, args) = timerCallbacks[id]!!
                    callback(args)
                }
                Event.eventType.CONNECTION_BUILD -> {
                    val endpoint1 = e.information["endpointId1"]!! as String
                    val endpoint2 = e.information["endpointId2"]!! as String
                    val protocol1 = endpointId2Protocols[endpoint1]!!
                    val protocol2 = endpointId2Protocols[endpoint2]!!
                    val delay = e.information["delay"]!! as Double
                    val bandwidth = e.information["bandwidth"]!! as Double
                    val (channel1, channel2) = createNetworkChannel(delay, bandwidth, endpoint1, endpoint2)
                    port2EndpointId[channel1.info()] = endpoint1
                    port2EndpointId[channel2.info()] = endpoint2
                    protocol1.onConnectionBuild(endpoint2, channel1)
                    protocol2.onConnectionBuild(endpoint1, channel2)
                    //println(endpoint1+" and "+endpoint2+" built a connection. Using channel"+channel1.info().toString()+" and "+channel2.info().toString())
                }
                Event.eventType.CONNECTION_LOST -> {
                    val endpoint1 = e.information["endpointId1"]!! as String
                    val endpoint2 = e.information["endpointId2"]!! as String
                    val protocol1 = endpointId2Protocols[endpoint1]!!
                    val protocol2 = endpointId2Protocols[endpoint2]!!
                    protocol1.onDisconnected(endpoint2)
                    protocol2.onDisconnected(endpoint1)
                }
                Event.eventType.USER_ADD_TRANSACTION -> {
                    val endpoint = e.information["endpointId"]!! as String
                    val protocol = endpointId2Protocols[endpoint]!!
                    val transaction = e.information["transaction"]!! as Transaction
                    protocol.onTransactionAdded(transaction)
                }
                Event.eventType.CONNECTION_CLOSED -> {
                    val id = e.information["id"]!! as Int
                    val port = e.information["port"]!! as Int
                    val delay = port2networkChannel[Pair(id, port)]!!.delay()
                    val endpoint = port2EndpointId[Pair(id, 1 - port)]!!
                    val protocol = endpointId2Protocols[endpoint]!!
                    timer(delay, { _: argsType -> protocol.onDisconnected(port2EndpointId[Pair(id, port)]!!) }, null)
                }
            }
            if (time > endTime)
                return
        }
    }

    fun <T : Any> createChannel(name1: String = "undefined", name2: String = "undefined"): Pair<Channel<T>, Channel<T>> {
        val id = nextID()
        val ch0 = Channel<T>(this, id, 0, name1)
        val ch1 = Channel<T>(this, id, 1, name2)
        port2Channel[ch0.info()] = ch0
        port2Channel[ch1.info()] = ch1
        return Pair(ch0, ch1)
    }

    private fun createNetworkChannel(delay: Double, bandwidth: Double, name1: String = "undefined", name2: String = "undefined"): Pair<networkChannel, networkChannel> {
        val id = nextID()
        val ch0 = networkChannel(this, id, 0, delay, bandwidth, name1)
        val ch1 = networkChannel(this, id, 1, delay, bandwidth, name2)
        port2networkChannel[ch0.info()] = ch0
        port2networkChannel[ch1.info()] = ch1
        return Pair(ch0, ch1)
    }


    fun timer(duration: Double, callback: (argsType) -> Unit, args: argsType) {
        val triggerTime = time + duration
        val information = HashMap<String, Any>()
        val id = nextID()
        information["id"] = id
        timerCallbacks[id] = Pair(callback, args)
        addEvent(Event(Event.eventType.TIMER_EXPIRED, triggerTime, information))
    }

    fun time(): Double {
        return time
    }

    fun regist(endpointID: String, protocol: Protocol) {
        endpointId2Protocols[endpointID] = protocol
    }

    private fun nextID(): Int {
        idSofar++
        return idSofar
    }

    fun dbgLvl(): debugLevel {
        return dbgLvl
    }
}

