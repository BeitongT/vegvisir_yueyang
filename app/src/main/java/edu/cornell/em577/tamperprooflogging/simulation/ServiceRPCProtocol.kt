package edu.cornell.em577.tamperprooflogging.simulation

import com.vegvisir.data.ProtocolMessageProto
import edu.cornell.em577.tamperprooflogging.data.exception.SignedBlockNotFoundException
import edu.cornell.em577.tamperprooflogging.data.model.SignedBlock
import edu.cornell.em577.tamperprooflogging.data.source.BoomerFilter
import com.google.android.gms.nearby.connection.ConnectionsClient.MAX_BYTES_DATA_SIZE

class ServiceRPCProtocol(
        private val sim: Simulator,
        private val connection: networkChannel,
        private val request: Channel<Message>,
        private val blockRepo: blockRepository,
        private val userRepo:userRepository,
        private val userPassword:String,
        private val localTimestamp: Long
) {
    companion object {
        val headerSize = ProtocolMessageProto.ProtocolMessage.newBuilder()
                .setType(ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_BLOCKS_RESPONSE).build().serializedSize
    }

    fun start() {
        request.onReceive(::onRequestReceived,null)
    }

    private fun onRequestReceived(m:Message,args:argsType){
        val r=m.asProtoMessage()
        try {
            when (r.type) {
                ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_ROOT_BLOCK_REQUEST ->
                    serviceGetRemoteRootBlock()
                ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_BLOCKS_REQUEST ->
                    serviceGetRemoteBlocks(r.getRemoteBlocksRequest.cryptoHashesList)
                ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_PROOF_OF_WITNESS_BLOCK_REQUEST ->
                    serviceGetRemoteProofOfWitnessBlock(r.getRemoteProofOfWitnessBlockRequest.parentHashesList)
                ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_TIMESTAMP_REQUEST ->
                    serviceGetRemoteTimestamp()
                ProtocolMessageProto.ProtocolMessage.MessageType.START_STREAM ->
                    serviceStartStream(BoomerFilter.Filter.fromProto(r.streamRequest.filter))
                ProtocolMessageProto.ProtocolMessage.MessageType.MERGE_COMPLETE,
                ProtocolMessageProto.ProtocolMessage.MessageType.MERGE_INTERRUPTED -> return
            }
        }
        catch (e:Exception){
            return
        }
        request.onReceive(::onRequestReceived,null)
    }

    private fun serviceGetRemoteRootBlock() {
        try {
            val rootBlock = blockRepo.getRootBlock()
            val response = ProtocolMessageProto.ProtocolMessage.newBuilder()
                    .setType(ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_ROOT_BLOCK_RESPONSE)
                    .setGetRemoteRootBlockResponse(
                            ProtocolMessageProto.GetRemoteRootBlockResponse.newBuilder()
                                    .setRemoteRootBlock(rootBlock.toProto())
                                    .build())
                    .build()
            connection.send(Message(response))

        } catch (sbnfe: SignedBlockNotFoundException) {
            val response = ProtocolMessageProto.ProtocolMessage.newBuilder()
                    .setType(ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_ROOT_BLOCK_RESPONSE)
                    .setGetRemoteRootBlockResponse(
                            ProtocolMessageProto.GetRemoteRootBlockResponse.newBuilder()
                                    .setFailedToRetrieve(true)
                                    .build())
                    .build()
            connection.send(Message(response))
        }
    }

    /** Services a request to fetch the local blocks given the crypto hashes of the blocks. */
    private fun serviceGetRemoteBlocks(cryptoHashes: List<String>) {
        try {
            val blocks = blockRepo.getBlocks(cryptoHashes)
            val getRemoteBlockResponse = ProtocolMessageProto.GetRemoteBlocksResponse.newBuilder()
                    .addAllRemoteBlocks(blocks.map { it.toProto() })
                    .build()
            if (getRemoteBlockResponse.serializedSize <= MAX_BYTES_DATA_SIZE - headerSize - 10) {
                val response = ProtocolMessageProto.ProtocolMessage.newBuilder()
                        .setType(ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_BLOCKS_RESPONSE)
                        .setGetRemoteBlocksResponse(getRemoteBlockResponse).build()
                connection.send(Message(response))
            } else {
                val response = ProtocolMessageProto.ProtocolMessage.newBuilder()
                        .setType(ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_BLOCKS_RESPONSE)
                        .setGetRemoteBlocksResponse(
                                ProtocolMessageProto.GetRemoteBlocksResponse.newBuilder()
                                        .setInStream(true)
                                        .build()
                        ).build()
                connection.send(Message(response))
                connection.syncStreamWrapper(ArrayList(blocks), { _: argsType -> connection.send(Message(listOf<SignedBlock>())) }, null)
            }
        } catch (sbnfe: SignedBlockNotFoundException) {
            val response = ProtocolMessageProto.ProtocolMessage.newBuilder()
                    .setType(ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_BLOCKS_RESPONSE)
                    .setGetRemoteBlocksResponse(
                            ProtocolMessageProto.GetRemoteBlocksResponse.newBuilder()
                                    .setFailedToRetrieve(true)
                                    .build()
                    ).build()
            connection.send(Message(response))
        }
    }

    /** Services a request to fetch a local proof of witness block given the parent hashes. */
    private fun serviceGetRemoteProofOfWitnessBlock(parentHashes: List<String>) {
        val proofOfWitness = SignedBlock.generatePOW4sim(
                userRepo, userPassword, parentHashes, localTimestamp)
        val response = ProtocolMessageProto.ProtocolMessage.newBuilder()
                .setType(ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_PROOF_OF_WITNESS_BLOCK_RESPONSE)
                .setGetRemoteProofOfWitnessBlockResponse(
                        ProtocolMessageProto.GetRemoteProofOfWitnessBlockResponse.newBuilder()
                                .setRemoteProofOfWitnessBlock(proofOfWitness.toProto())
                                .build()
                ).build()
        connection.send(Message(response))
    }

    /** Services a request to fetch the local timestamp. */
    private fun serviceGetRemoteTimestamp() {
        val response = ProtocolMessageProto.ProtocolMessage.newBuilder()
                .setType(ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_TIMESTAMP_RESPONSE)
                .setGetRemoteTimestampResponse(
                        ProtocolMessageProto.GetRemoteTimestampResponse.newBuilder()
                                .setRemoteTimestamp(localTimestamp)
                                .build()
                ).build()
        connection.send(Message(response))
    }

    private fun serviceStartStream(remotefilter: BoomerFilter.Filter) {
        try {
            val hashToSend = blockRepo.boomer.testFilter(remotefilter)
            connection.syncStreamWrapper(ArrayList(blockRepo.getBlocks(hashToSend)), { _: argsType -> /*connection.send(Message(listOf<SignedBlock>()))*/ }, null)
        }
        catch (sbnfe: SignedBlockNotFoundException) {
            val response = ProtocolMessageProto.ProtocolMessage.newBuilder()
                    .setType(ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_BLOCKS_RESPONSE)
                    .setGetRemoteBlocksResponse(
                            ProtocolMessageProto.GetRemoteBlocksResponse.newBuilder()
                                    .setFailedToRetrieve(true)
                                    .build()
                    ).build()
            connection.send(Message(response))
        }
    }
}