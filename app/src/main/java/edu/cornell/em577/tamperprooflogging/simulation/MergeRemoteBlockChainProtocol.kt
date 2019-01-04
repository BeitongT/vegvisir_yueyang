package edu.cornell.em577.tamperprooflogging.simulation

import android.util.Log
import com.vegvisir.data.ProtocolMessageProto
import edu.cornell.em577.tamperprooflogging.data.model.SignedBlock
import edu.cornell.em577.tamperprooflogging.protocol.exception.UnexpectedTerminationException

class MergeRemoteBlockChainProtocol(
        private val sim: Simulator,
        private val connection: networkChannel,
        private val response: Channel<Message>,
        private val stream: Channel<Channel<Message>>,
        private val blockRepo: blockRepository,
        private val userRepo: userRepository,
        private val userId: String,
        private val userPassword: String,
        private val localTimestamp: Long
) {
    companion object {
        const val loglevel = 1
    }

    fun start() {
        when (Protocol.protocolType) {
            Protocol.simple -> simple()
            Protocol.bloomfilter -> withBloomFilter()
            else -> {
                throw RuntimeException("wtf")
            }
        }
    }

    private fun simple() {
        val currentRootBlock = blockRepo.getRootBlock()
        val remoteRootBlockCallBack = { remoteRootBlock: SignedBlock, _: argsType ->
            if (currentRootBlock.cryptoHash == remoteRootBlock.cryptoHash)
                mergeCleanup()
            else {
                val frontierHashes = ArrayList<String>()
                if (!blockRepo.containsBlock(remoteRootBlock.cryptoHash))
                    frontierHashes.add(remoteRootBlock.cryptoHash)
                val streamResult = Pair(HashMap(listOf(remoteRootBlock).map { it.cryptoHash to it }.toMap()), setOf(remoteRootBlock.cryptoHash))
                val fillingGapPhaseCallBack = { blocksToAddByCryptoHash: HashMap<String, SignedBlock>, seenCurrentRoot: Boolean, _: argsType ->
                    if (!seenCurrentRoot) frontierHashes.add(currentRootBlock.cryptoHash)
                    val addPOWCallBack = { POW: SignedBlock, _: argsType ->
                        blockRepo.updateBlockChain(blocksToAddByCryptoHash.values.toList(), POW)
                        mergeCleanup()
                    }
                    addProofOfWitnessBlocks(blocksToAddByCryptoHash, frontierHashes, currentRootBlock, remoteRootBlock, addPOWCallBack, null)
                }
                fillingGapPhase(streamResult, arrayOf(currentRootBlock.cryptoHash, Pair(fillingGapPhaseCallBack, null)))
            }
        }
        getRemoteRootBlock(remoteRootBlockCallBack, null)
    }

    private fun withBloomFilter() {
        val currentRootBlock = blockRepo.getRootBlock()
        val remoteRootBlockCallBack = { remoteRootBlock: SignedBlock, _: argsType ->
            log("got remote root block")
            if (currentRootBlock.cryptoHash == remoteRootBlock.cryptoHash)
                mergeCleanup()
            else {
                val frontierHashes = ArrayList<String>()
                if (!blockRepo.containsBlock(remoteRootBlock.cryptoHash))
                    frontierHashes.add(remoteRootBlock.cryptoHash)
                val streamingPhaseCallBack = { streamResult: Pair<HashMap<String, SignedBlock>, Set<String>>, _: argsType ->
                    log("start filling gap phase")
                    val fillingGapPhaseCallBack = { blocksToAddByCryptoHash: HashMap<String, SignedBlock>, seenCurrentRoot: Boolean, _: argsType ->
                        log("adding Pow")
                        if (!seenCurrentRoot) frontierHashes.add(currentRootBlock.cryptoHash)
                        val addPOWCallBack = { POW: SignedBlock, _: argsType ->
                            blockRepo.updateBlockChain(blocksToAddByCryptoHash.values.toList(), POW)
                            mergeCleanup()
                        }
                        addProofOfWitnessBlocks(blocksToAddByCryptoHash, frontierHashes, currentRootBlock, remoteRootBlock, addPOWCallBack, null)
                    }
                    fillingGapPhase(streamResult, arrayOf(currentRootBlock.cryptoHash, Pair(fillingGapPhaseCallBack, null)))
                }
                log("start stream phase")
                streamingPhase(remoteRootBlock.cryptoHash, arrayOf(Pair(streamingPhaseCallBack, null)))
            }
        }
        getRemoteRootBlock(remoteRootBlockCallBack, null)
    }

    private fun addProofOfWitnessBlocks(
            blocksToAddByCryptoHash: HashMap<String, SignedBlock>,
            frontierHashes: List<String>,
            currentRootBlock: SignedBlock,
            remoteRootBlock: SignedBlock,
            callback: (SignedBlock, argsType) -> Unit,
            args: argsType
    ) {
        if (blockRepo.containsBlock(remoteRootBlock.cryptoHash)) {
            val remotePOWCallBack = { remoteProofOfWitnessBlock: SignedBlock, _: argsType ->
                blocksToAddByCryptoHash[remoteProofOfWitnessBlock.cryptoHash] = remoteProofOfWitnessBlock
                callback(remoteProofOfWitnessBlock, args)
            }
            getRemoteProofOfWitnessBlock(frontierHashes, remotePOWCallBack, null)
        } else if (currentRootBlock.cryptoHash !in frontierHashes) {
            val localProofOfWitnessBlock = SignedBlock.generatePOW4sim(
                    userRepo,
                    userPassword,
                    frontierHashes,
                    localTimestamp
            )
            blocksToAddByCryptoHash[localProofOfWitnessBlock.cryptoHash] = localProofOfWitnessBlock
            callback(localProofOfWitnessBlock, args)
        } else {
            val remoteTimestampCallBack = { remoteTimestamp: Long, _: argsType ->
                if (remoteTimestamp > localTimestamp) {
                    val localProofOfWitnessBlock = SignedBlock.generatePOW4sim(
                            userRepo,
                            userPassword,
                            frontierHashes,
                            localTimestamp
                    )
                    val remotePOWCallBack = { remoteProofOfWitnessBlock: SignedBlock, _: argsType ->
                        blocksToAddByCryptoHash[remoteProofOfWitnessBlock.cryptoHash] =
                                remoteProofOfWitnessBlock
                        blocksToAddByCryptoHash[localProofOfWitnessBlock.cryptoHash] = localProofOfWitnessBlock
                        callback(remoteProofOfWitnessBlock, args)
                    }
                    getRemoteProofOfWitnessBlock(listOf(localProofOfWitnessBlock.cryptoHash), remotePOWCallBack, null)
                } else {
                    val remotePOWCallBack = { remoteProofOfWitnessBlock: SignedBlock, _: argsType ->
                        val localProofOfWitnessBlock = SignedBlock.generatePOW4sim(
                                userRepo,
                                userPassword,
                                listOf(remoteProofOfWitnessBlock.cryptoHash),
                                localTimestamp
                        )
                        blocksToAddByCryptoHash[remoteProofOfWitnessBlock.cryptoHash] = remoteProofOfWitnessBlock
                        blocksToAddByCryptoHash[localProofOfWitnessBlock.cryptoHash] = localProofOfWitnessBlock
                        callback(localProofOfWitnessBlock, args)
                    }
                    getRemoteProofOfWitnessBlock(frontierHashes, remotePOWCallBack, null)
                }
            }
            getRemoteTimestamp(remoteTimestampCallBack, null)
        }
    }

    private fun streamingPhase(remoteRootHash: String, args: argsType) {
        val request = ProtocolMessageProto.ProtocolMessage.newBuilder()
                .setType(ProtocolMessageProto.ProtocolMessage.MessageType.START_STREAM)
                .setStreamRequest(ProtocolMessageProto.StreamRequest.newBuilder()
                        .setFilter(blockRepo.boomer.getFilter(blockRepo.getRootBlock().cryptoHash)!!.toProto())
                        .build()
                )
                .build()
        connection.send(Message(request))
        args!!
        val (callback, extra) = args[0] as Pair<(Pair<HashMap<String, SignedBlock>, Set<String>>, argsType) -> Unit, argsType>

        val receiveFromStreamAndCallback = { ch: Channel<Message>, _: argsType ->
            val blocks = ArrayList<SignedBlock>()
            val afterReceived = { _: argsType ->
                val received = HashMap(blocks.map { it.cryptoHash to it }.toMap())
                val exist = HashSet<String>()
                val need = HashSet<String>(listOf(remoteRootHash))
                blocks.forEach {
                    exist.add(it.cryptoHash)
                    need.addAll(it.unsignedBlock.parentHashes)
                }
                callback(Pair(received, need - exist), extra)
            }
            ch.setCloseCB(afterReceived,null)
            ch.onReceive(::onReceivedStream, arrayOf(ch, blocks))
        }
        stream.onReceive(receiveFromStreamAndCallback, null)
    }

    private fun fillingGapPhase(streamResult: Pair<HashMap<String, SignedBlock>, Set<String>>, args: argsType) {
        args!!
        val (blocksToAddByCryptoHash, backTier) = streamResult
        val rootHash = args[0] as String
        val (callback, extra) = args[1] as Pair<(HashMap<String, SignedBlock>, Boolean, argsType) -> Unit, argsType>
        val hasRoot = backTier.contains(rootHash)
        val blocksToFetch = ArrayList<String>(backTier.filter { !blockRepo.containsBlock(it) })
        if (blocksToFetch.isNotEmpty())
            getRemoteBlocks(blocksToFetch, ::fillingGapOnBlocksReceived, arrayOf(blocksToAddByCryptoHash, rootHash, hasRoot, Pair(callback, extra)))
        else
            callback(blocksToAddByCryptoHash, hasRoot, extra)
    }

    private fun fillingGapOnBlocksReceived(blocks: List<SignedBlock>, args: argsType) {
        args!!
        val blocksToAddByCryptoHash = args[0] as HashMap<String, SignedBlock>
        val rootHash = args[1] as String
        var hasRoot = args[2] as Boolean
        val (callback, extra) = args[3] as Pair<(HashMap<String, SignedBlock>, Boolean, argsType) -> Unit, argsType>
        val blocksToFetch = ArrayList<String>()
        blocks.forEach {
            blocksToAddByCryptoHash.put(it.cryptoHash, it)
        }
        blocks.forEach {
            blocksToFetch.addAll(it.unsignedBlock.parentHashes.filter { !blockRepo.containsBlock(it) && !blocksToAddByCryptoHash.containsKey(it) })
            hasRoot = hasRoot || it.unsignedBlock.parentHashes.contains(rootHash)
        }
        if (blocksToFetch.isEmpty())
            callback(blocksToAddByCryptoHash, hasRoot, extra)
        else
            getRemoteBlocks(blocksToFetch, ::fillingGapOnBlocksReceived, arrayOf(blocksToAddByCryptoHash, rootHash, hasRoot, Pair(callback, extra)))

    }

    private fun onReceivedStream(m: Message, args: argsType) {
        args!!
        val ch = args[0] as Channel<Message>
        val blocks = args[1] as ArrayList<SignedBlock>
        blocks.addAll(m.asBlocks())
        ch.onReceive(::onReceivedStream, arrayOf(ch, blocks))
    }

    private fun mergeCleanup() {
        println(userId + ": Merge clean up")
        val mergeCompleteMessage = ProtocolMessageProto.ProtocolMessage.newBuilder()
                .setType(ProtocolMessageProto.ProtocolMessage.MessageType.MERGE_COMPLETE)
                .setNoBody(true)
                .build()
        connection.send(Message(mergeCompleteMessage))
    }

    private fun getRemoteRootBlock(callback: ((SignedBlock, argsType) -> Unit), args: argsType) {
        val request = ProtocolMessageProto.ProtocolMessage.newBuilder()
                .setType(ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_ROOT_BLOCK_REQUEST)
                .setNoBody(true)
                .build()
        connection.send(Message(request))
        val ongetRemoteRootBlockResponse = { m: Message, _: argsType ->
            val response = m.asProtoMessage()
            if (response.type == ProtocolMessageProto.ProtocolMessage.MessageType.MERGE_INTERRUPTED) {
                OnConnectionError()
            } else if (response.type ==
                    ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_ROOT_BLOCK_RESPONSE
            ) {
                if (response.getRemoteRootBlockResponse.failedToRetrieve) {
                    OnConnectionError()
                }
                callback(SignedBlock.fromProto(response.getRemoteRootBlockResponse.remoteRootBlock), args)
            }
        }
        response.onReceive(ongetRemoteRootBlockResponse, null)
    }

    private fun getRemoteBlocks(cryptoHashes: List<String>, callback: (List<SignedBlock>, argsType) -> Unit, args: argsType) {
        val request = ProtocolMessageProto.ProtocolMessage.newBuilder()
                .setType(ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_BLOCKS_REQUEST)
                .setGetRemoteBlocksRequest(
                        ProtocolMessageProto.GetRemoteBlocksRequest.newBuilder()
                                .addAllCryptoHashes(cryptoHashes)
                                .build()
                ).build()

        connection.send(Message(request))
        val onGetRemoteBlocksResponse = { m: Message, _: argsType ->
            val response = m.asProtoMessage()
            if (response.type == ProtocolMessageProto.ProtocolMessage.MessageType.MERGE_INTERRUPTED) {
                OnConnectionError()
            } else if (response.type ==
                    ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_BLOCKS_RESPONSE
            ) {
                if (response.getRemoteBlocksResponse.failedToRetrieve) {
                    OnConnectionError()
                }
                val afterReceived = { result: List<SignedBlock>, _: argsType ->
                    if (result.map { it.cryptoHash }.toSet() != cryptoHashes.toSet()) {
                        OnConnectionError()
                    }
                    callback(result, args)
                }
                if (response.getRemoteBlocksResponse.inStream) {
                    val receiveFromStreamAndCallback = { ch: Channel<Message>, _: argsType ->
                        ch.onReceive(::onReceivedStream, arrayOf(ch, ArrayList<SignedBlock>(), Pair(afterReceived, null)))
                    }
                    stream.onReceive(receiveFromStreamAndCallback, arrayOf(Pair(
                            afterReceived, null)))
                } else {
                    val result = response.getRemoteBlocksResponse.remoteBlocksList.map {
                        SignedBlock.fromProto(
                                it
                        )
                    }
                    afterReceived(result, null)
                }
            }
        }
        response.onReceive(onGetRemoteBlocksResponse, null)
    }

    private fun getRemoteProofOfWitnessBlock(parentHashes: List<String>, callback: (SignedBlock, argsType) -> Unit, args: argsType) {
        val request = ProtocolMessageProto.ProtocolMessage.newBuilder()
                .setType(ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_PROOF_OF_WITNESS_BLOCK_REQUEST)
                .setGetRemoteProofOfWitnessBlockRequest(
                        ProtocolMessageProto.GetRemoteProofOfWitnessBlockRequest.newBuilder()
                                .addAllParentHashes(parentHashes)
                                .build()
                ).build()
        connection.send(Message(request))
        val onGetRemoteProofOfWitnessBlockResponse = { m: Message, _: argsType ->
            val response = m.asProtoMessage()
            if (response.type == ProtocolMessageProto.ProtocolMessage.MessageType.MERGE_INTERRUPTED) {
                OnConnectionError()
            } else if (response.type ==
                    ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_PROOF_OF_WITNESS_BLOCK_RESPONSE
            ) {
                val result = SignedBlock.fromProto(response.getRemoteProofOfWitnessBlockResponse.remoteProofOfWitnessBlock)
                if (result.unsignedBlock.parentHashes.toSet() != parentHashes.toSet()) {
                    OnConnectionError()
                }
                callback(result, args)
            }
        }
        response.onReceive(onGetRemoteProofOfWitnessBlockResponse, null)
    }

    private fun getRemoteTimestamp(callback: (Long, argsType) -> Unit, args: argsType) {
        val request = ProtocolMessageProto.ProtocolMessage.newBuilder()
                .setType(ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_TIMESTAMP_REQUEST)
                .setNoBody(true)
                .build()
        connection.send(Message(request))
        val onGetRemoteTimestampResponse = { m: Message, _: argsType ->
            val response = m.asProtoMessage()
            if (response.type == ProtocolMessageProto.ProtocolMessage.MessageType.MERGE_INTERRUPTED) {
                OnConnectionError()
            } else if (response.type ==
                    ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_TIMESTAMP_RESPONSE
            ) {
                callback(response.getRemoteTimestampResponse.remoteTimestamp, args)
            } else OnException("error on order")
        }
        response.onReceive(onGetRemoteTimestampResponse, null)
    }


    private fun OnConnectionError() {
        log("early terminated")
    }

    private fun OnException(info: String) {

    }

    private fun log(s: String) {
        if (loglevel == 1)
            println("$userId : $s")
    }
}