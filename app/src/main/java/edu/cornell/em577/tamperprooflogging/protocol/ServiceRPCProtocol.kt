package edu.cornell.em577.tamperprooflogging.protocol

import android.util.Log
import com.vegvisir.data.ProtocolMessageProto
import edu.cornell.em577.tamperprooflogging.data.exception.SignedBlockNotFoundException
import edu.cornell.em577.tamperprooflogging.data.model.SignedBlock
import edu.cornell.em577.tamperprooflogging.data.source.BlockRepository
import edu.cornell.em577.tamperprooflogging.data.source.BoomerFilter
import edu.cornell.em577.tamperprooflogging.data.source.UserDataRepository
import edu.cornell.em577.tamperprooflogging.network.ByteStream
import java.io.File
import java.io.PipedInputStream
import java.io.PipedOutputStream
import java.nio.ByteBuffer
import java.util.concurrent.LinkedBlockingQueue
import com.google.android.gms.nearby.connection.ConnectionsClient

/** Protocol for servicing remote procedure calls made by the remote endpoint */
class ServiceRPCProtocol(
        private val byteStream: ByteStream,
        private val endpointId: String,
        private val blockRepo: BlockRepository,
        private val userRepo: UserDataRepository,
        private val userPassword: String,
        private val localTimestamp: Long
) : Runnable {

    companion object {
        val headerSize = ProtocolMessageProto.ProtocolMessage.newBuilder()
                .setType(ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_BLOCKS_RESPONSE).build().serializedSize
    }

    val requestChannel = LinkedBlockingQueue<ProtocolMessageProto.ProtocolMessage>()
    /**
     * Listens for messages from the dispatcher, services the requests and sends the response to
     * the remote endpoint.
     */
    override fun run() {
        listener@ while (true) {
            try {
                val request = requestChannel.take()
                when (request.type) {
                    ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_ROOT_BLOCK_REQUEST ->
                        serviceGetRemoteRootBlock()
                    ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_BLOCKS_REQUEST ->
                        serviceGetRemoteBlocks(request.getRemoteBlocksRequest.cryptoHashesList)
                    ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_PROOF_OF_WITNESS_BLOCK_REQUEST ->
                        serviceGetRemoteProofOfWitnessBlock(request.getRemoteProofOfWitnessBlockRequest.parentHashesList)
                    ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_TIMESTAMP_REQUEST ->
                        serviceGetRemoteTimestamp()
                    ProtocolMessageProto.ProtocolMessage.MessageType.START_STREAM ->
                        serviceStartStream(BoomerFilter.Filter.fromProto(request.streamRequest.filter))
                    ProtocolMessageProto.ProtocolMessage.MessageType.MERGE_COMPLETE,
                    ProtocolMessageProto.ProtocolMessage.MessageType.MERGE_INTERRUPTED -> break@listener
                    else -> continue@listener
                }
            } catch (e: Exception) {
                break
            }
        }
    }

    /** Services a request to fetch the local root block. */
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
                    .toByteArray()
            byteStream.send(endpointId, response)

        } catch (sbnfe: SignedBlockNotFoundException) {
            val response = ProtocolMessageProto.ProtocolMessage.newBuilder()
                    .setType(ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_ROOT_BLOCK_RESPONSE)
                    .setGetRemoteRootBlockResponse(
                            ProtocolMessageProto.GetRemoteRootBlockResponse.newBuilder()
                                    .setFailedToRetrieve(true)
                                    .build())
                    .build()
                    .toByteArray()

            byteStream.send(endpointId, response)
        }
    }

    /** Services a request to fetch the local blocks given the crypto hashes of the blocks. */
    private fun serviceGetRemoteBlocks(cryptoHashes: List<String>) {
        try {
            val blocks = blockRepo.getBlocks(cryptoHashes)
            val getRemoteBlockResponse = ProtocolMessageProto.GetRemoteBlocksResponse.newBuilder()
                    .addAllRemoteBlocks(blocks.map { it.toProto() })
                    .build()
            if (getRemoteBlockResponse.serializedSize <= ConnectionsClient.MAX_BYTES_DATA_SIZE - headerSize - 10) {
                Log.d("Checking","In one chunk")
                val response = ProtocolMessageProto.ProtocolMessage.newBuilder()
                        .setType(ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_BLOCKS_RESPONSE)
                        .setGetRemoteBlocksResponse(getRemoteBlockResponse).build()
                        .toByteArray()
                byteStream.send(endpointId, response)
            } else {
                Log.d("Checking","In stream")
                val response = ProtocolMessageProto.ProtocolMessage.newBuilder()
                        .setType(ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_BLOCKS_RESPONSE)
                        .setGetRemoteBlocksResponse(
                                ProtocolMessageProto.GetRemoteBlocksResponse.newBuilder()
                                        .setInStream(true)
                                        .build()
                        ).build()
                        .toByteArray()
                byteStream.send(endpointId,response)
                val outS = PipedOutputStream()
                val inS = PipedInputStream(outS)
                byteStream.sendStream(endpointId, inS)
                val bb = ByteBuffer.allocate(4)
                blocks.forEach {
                    bb.clear()
                    val blockBytes = it.toProto().toByteArray()
                    outS.write(bb.putInt(blockBytes.size).array())
                    outS.write(it.toProto().toByteArray())
                }
                outS.close()
            }
        } catch (sbnfe: SignedBlockNotFoundException) {
            val response = ProtocolMessageProto.ProtocolMessage.newBuilder()
                    .setType(ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_BLOCKS_RESPONSE)
                    .setGetRemoteBlocksResponse(
                            ProtocolMessageProto.GetRemoteBlocksResponse.newBuilder()
                                    .setFailedToRetrieve(true)
                                    .build()
                    ).build()
                    .toByteArray()

            byteStream.send(endpointId, response)
        }
    }

    /** Services a request to fetch a local proof of witness block given the parent hashes. */
    private fun serviceGetRemoteProofOfWitnessBlock(parentHashes: List<String>) {
        val proofOfWitness = SignedBlock.generateProofOfWitness(
                userRepo, userPassword, parentHashes, localTimestamp)
        val response = ProtocolMessageProto.ProtocolMessage.newBuilder()
                .setType(ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_PROOF_OF_WITNESS_BLOCK_RESPONSE)
                .setGetRemoteProofOfWitnessBlockResponse(
                        ProtocolMessageProto.GetRemoteProofOfWitnessBlockResponse.newBuilder()
                                .setRemoteProofOfWitnessBlock(proofOfWitness.toProto())
                                .build()
                ).build()
                .toByteArray()

        byteStream.send(endpointId, response)
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
                .toByteArray()

        byteStream.send(endpointId, response)
    }

    private fun serviceStartStream(remotefilter: BoomerFilter.Filter) {
        Log.d("Checking", "Being asked for streaming")
        val hashToSend = blockRepo.boomer.testFilter(remotefilter)
        /*val tempFile= File.createTempFile("vegvisir","")
        val bb=ByteBuffer.allocate(4)
        blockRepo.getBlocks(hashToSend).forEach {
            bb.clear()
            val blockBytes=it.toProto().toByteArray()
            tempFile.writeBytes(bb.putInt(blockBytes.size).array())
            tempFile.writeBytes(it.toProto().toByteArray())
            Log.d("Checking","Add a new block to stream")
        }
        Log.d("Checking","File prepared")
        byteStream.sendTempFile(endpointId,tempFile)*/
        val outS = PipedOutputStream()
        val inS = PipedInputStream(outS)
        byteStream.sendStream(endpointId, inS)
        Log.d("Checking", "Stream Sent")
        val bb = ByteBuffer.allocate(4)
        blockRepo.getBlocks(hashToSend).forEach {
            bb.clear()
            val blockBytes = it.toProto().toByteArray()
            outS.write(bb.putInt(blockBytes.size).array())
            outS.write(it.toProto().toByteArray())
            Log.d("Checking", "Add a new block to stream")
        }
        outS.close()
        Log.d("Checking", "OutStream closed")
    }
}