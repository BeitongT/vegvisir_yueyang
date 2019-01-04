package edu.cornell.em577.tamperprooflogging.simulation

import com.vegvisir.data.ProtocolMessageProto

class EstablishRemoteExchangeProtocol(
        private val sim: Simulator,
        private val userId: String,
        private val userPassword: String,
        private val establishedConnection: Channel<Pair<Channel<Message>, networkChannel>>,
        private val blockRepo: blockRepository
) {
    companion object {
        private const val WAIT_TIME_IN_SECONDS = 60.toDouble()
        private const val SECOND_IN_MILLI = 1000.toDouble()
        private const val loglevel=1
    }

    private var localCompleted = false
    private var remoteCompleted = false
    private var streamChannel: Channel<Message>? = null
//    private var afterCompleted:Pair<((Int,argsType)->Unit),argsType>=Pair({_:Int,_:argsType-> },null)

    fun start() {
        whileLoop(null)
    }

//    fun join(callback:(Int,argsType)->Unit,args:argsType){
//        afterCompleted
//    }

    private fun whileLoop(args: argsType) {
        log("Establishing Connection")
        establishedConnection.onReceive(::afterConnectionEstablished, null)
    }

    private fun afterConnectionEstablished(p: Pair<Channel<Message>, networkChannel>, args: argsType) {
        log("Established Connection")
        val (connectionChannel, networkConnection) = p
        val (requestIn, requestOut) = sim.createChannel<Message>()
        val (responseIn, responseOut) = sim.createChannel<Message>()
        val (streamIn, streamOut) = sim.createChannel<Channel<Message>>()
        val timestamp = sim.time().toLong()
        val mergeProtocol = MergeRemoteBlockChainProtocol(sim, networkConnection, responseOut, streamOut, blockRepo, blockRepo.userRepo, userId, userPassword, timestamp)
        val serviceProtocol = ServiceRPCProtocol(sim, networkConnection, requestOut, blockRepo, blockRepo.userRepo, userPassword, timestamp)
        mergeProtocol.start()
        serviceProtocol.start()
        connectionChannel.onReceive(::onProtoMessageReceived, arrayOf(connectionChannel, requestIn, responseIn, streamIn, networkConnection))

    }

    private fun onProtoMessageReceived(m: Message, args: argsType) {
        args!!
        val connectionChannel = args[0] as Channel<Message>
        val requestIn = args[1] as Channel<Message>
        val responseIn = args[2] as Channel<Message>
        val streamIn = args[3] as Channel<Channel<Message>>
        val network = args[4] as networkChannel
        when (m.type) {
            Message.messageType.PROTOMESSAGE -> {
                val protoM = m.asProtoMessage()
                when (protoM.type) {
                    ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_ROOT_BLOCK_REQUEST,
                    ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_BLOCKS_REQUEST,
                    ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_PROOF_OF_WITNESS_BLOCK_REQUEST,
                    ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_TIMESTAMP_REQUEST,
                    ProtocolMessageProto.ProtocolMessage.MessageType.START_STREAM ->
                        requestIn.send(m)
                    ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_ROOT_BLOCK_RESPONSE,
                    ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_BLOCKS_RESPONSE,
                    ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_PROOF_OF_WITNESS_BLOCK_RESPONSE,
                    ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_TIMESTAMP_RESPONSE ->
                        responseIn.send(m)
                    ProtocolMessageProto.ProtocolMessage.MessageType.MERGE_INTERRUPTED -> {
                        if(streamChannel!=null) {
                            log(" early terminated streaming")
                            streamChannel!!.close()
                            streamChannel = null
                        }
                        addInteruptMessage(requestIn, responseIn)
                        thingsAfterReconciliation(network)
                        return
                    }
                    ProtocolMessageProto.ProtocolMessage.MessageType.MERGE_COMPLETE -> {
                        val mergeCompleteAckMessage = ProtocolMessageProto.ProtocolMessage.newBuilder()
                                .setType(ProtocolMessageProto.ProtocolMessage.MessageType.MERGE_COMPLETE_ACK)
                                .setNoBody(true)
                                .build()
                        network.send(Message(mergeCompleteAckMessage))
                        requestIn.send(m)
                        remoteCompleted = true
                    }
                    ProtocolMessageProto.ProtocolMessage.MessageType.MERGE_COMPLETE_ACK -> {
                        localCompleted = true
                    }
                    else -> throw RuntimeException("Improperly formatted message received")
                }
            }
        // Assume there could be at most one streaming channel
            Message.messageType.BLOCKS -> {
                if (streamChannel == null) {
                    log(" start streaming")
                    val (ch0, ch1) = sim.createChannel<Message>()
                    streamIn.send(ch1)
                    streamChannel = ch0
                }
                if (m.asBlocks().isNotEmpty()) {
                    log(" get streaming")
                    streamChannel!!.send(m)
                } else {
                    log(" streaming ended")
                    streamChannel!!.close()
                    streamChannel = null
                }
            }
        }
        if (remoteCompleted && localCompleted) {
            thingsAfterReconciliation(network)
            return
        }
        connectionChannel.onReceive(::onProtoMessageReceived, arrayOf(connectionChannel, requestIn, responseIn, streamIn, network))
    }

    private fun addInteruptMessage(requestIn: Channel<Message>, responseIn: Channel<Message>) {
        val mergeInterruptedMessage = ProtocolMessageProto.ProtocolMessage.newBuilder()
                .setType(ProtocolMessageProto.ProtocolMessage.MessageType.MERGE_INTERRUPTED)
                .setNoBody(true)
                .build()
        val m = Message(mergeInterruptedMessage)
        responseIn.send(m)
        requestIn.send(m)
    }

    private fun thingsAfterReconciliation(network: networkChannel) {
        network.close()
        sim.timer(WAIT_TIME_IN_SECONDS * SECOND_IN_MILLI, ::whileLoop, null)
    }
    
    private fun log(s:String){
        if(loglevel==1)
            println("$userId : $s")
    }
}