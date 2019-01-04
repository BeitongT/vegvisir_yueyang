package edu.cornell.em577.tamperprooflogging.protocol

import android.util.Log
import com.vegvisir.data.ProtocolMessageProto
import edu.cornell.em577.tamperprooflogging.data.model.SignedBlock
import edu.cornell.em577.tamperprooflogging.data.source.BlockRepository
import edu.cornell.em577.tamperprooflogging.data.source.BoomerFilter
import edu.cornell.em577.tamperprooflogging.data.source.UserDataRepository
import edu.cornell.em577.tamperprooflogging.network.ByteStream
import edu.cornell.em577.tamperprooflogging.protocol.exception.UnexpectedTerminationException
import java.io.File
import java.io.InputStream
import java.nio.ByteBuffer
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.collections.HashSet

/** Protocol for merging a remote blockchain into the local blockchain */
class MergeRemoteBlockChainProtocol(
        private val byteStream: ByteStream,
        private val endpointId: String,
        private val blockRepo: BlockRepository,
        private val userRepo: UserDataRepository,
        private val userPassword: String,
        private val localTimestamp: Long
) : Runnable {

    @Volatile
    var completed = false

    val responseChannel = LinkedBlockingQueue<ProtocolMessageProto.ProtocolMessage>()

    val streamChannel = LinkedBlockingQueue<InputStream>()
    /**
     * Retrieves and return the collection of all remote blocks to add, indexed on their cryptographic hashes, as well
     * as the cryptographic hashes of the new frontier set.
     */
    private fun getRemoteBlocksToAdd(
            currentRootBlock: SignedBlock,
            remoteRootBlock: SignedBlock
    ): Pair<HashMap<String, SignedBlock>, List<String>> {

        val blocksToAddByCryptoHash = HashMap<String, SignedBlock>()
        val frontierHashes = ArrayList<String>()

        var seenCurrentRoot = false
        if (!blockRepo.containsBlock(remoteRootBlock.cryptoHash)) {
            frontierHashes.add(remoteRootBlock.cryptoHash)
            val stack = ArrayDeque<SignedBlock>(listOf(remoteRootBlock))

            while (stack.isNotEmpty()) {
                val current = stack.pop()
                blocksToAddByCryptoHash[current.cryptoHash] = current
                val blocksToFetch = ArrayList<String>()

                for (parentHash in current.unsignedBlock.parentHashes) {
                    if (parentHash == currentRootBlock.cryptoHash) {
                        seenCurrentRoot = true
                    }
                    if (!blockRepo.containsBlock(parentHash)) {
                        if (parentHash !in blocksToAddByCryptoHash) {
                            blocksToFetch.add(parentHash)
                        }
                    }
                }
                getRemoteBlocks(blocksToFetch).forEach({ stack.push(it) })
            }
        }
        if (!seenCurrentRoot) {
            frontierHashes.add(currentRootBlock.cryptoHash)
        }
        return Pair(blocksToAddByCryptoHash, frontierHashes)
    }

    /**
     * Populates the provided collection of blocks to add with the proof of witness blocks
     * Returns the new root node of the blockchain.
     */
    private fun addProofOfWitnessBlocks(
            blocksToAddByCryptoHash: HashMap<String, SignedBlock>,
            frontierHashes: List<String>,
            currentRootBlock: SignedBlock,
            remoteRootBlock: SignedBlock
    ): SignedBlock {
        if (blockRepo.containsBlock(remoteRootBlock.cryptoHash)) {
            Log.d("Checking", "Only remote POW")
            val remoteProofOfWitnessBlock = getRemoteProofOfWitnessBlock(frontierHashes)
            blocksToAddByCryptoHash[remoteProofOfWitnessBlock.cryptoHash] =
                    remoteProofOfWitnessBlock
            return remoteProofOfWitnessBlock
        } else if (currentRootBlock.cryptoHash !in frontierHashes) {
            Log.d("Checking", "Only local POW")
            val localProofOfWitnessBlock = SignedBlock.generateProofOfWitness(
                    userRepo,
                    userPassword,
                    frontierHashes,
                    localTimestamp
            )
            blocksToAddByCryptoHash[localProofOfWitnessBlock.cryptoHash] = localProofOfWitnessBlock
            return localProofOfWitnessBlock
        }
        Log.d("Checking", "Double POW")
        val remoteTimestamp = getRemoteTimestamp()
        return if (remoteTimestamp > localTimestamp) {
            val localProofOfWitnessBlock = SignedBlock.generateProofOfWitness(
                    userRepo,
                    userPassword,
                    frontierHashes,
                    localTimestamp
            )

            val remoteProofOfWitnessBlock =
                    getRemoteProofOfWitnessBlock(listOf(localProofOfWitnessBlock.cryptoHash))
            blocksToAddByCryptoHash[remoteProofOfWitnessBlock.cryptoHash] =
                    remoteProofOfWitnessBlock
            blocksToAddByCryptoHash[localProofOfWitnessBlock.cryptoHash] = localProofOfWitnessBlock
            remoteProofOfWitnessBlock
        } else {
            val remoteProofOfWitnessBlock = getRemoteProofOfWitnessBlock(frontierHashes)
            val localProofOfWitnessBlock = SignedBlock.generateProofOfWitness(
                    userRepo,
                    userPassword,
                    listOf(remoteProofOfWitnessBlock.cryptoHash),
                    localTimestamp
            )
            blocksToAddByCryptoHash[remoteProofOfWitnessBlock.cryptoHash] = remoteProofOfWitnessBlock
            blocksToAddByCryptoHash[localProofOfWitnessBlock.cryptoHash] = localProofOfWitnessBlock
            localProofOfWitnessBlock
        }
    }

    private fun read(s: InputStream, b: ByteArray, l: Int): Int {
        var i = 0
        while (i < l) {
            val n = s.read(b, i, l - i)
            if (n == -1) return -1
            i += n
        }
        return i
    }

    private fun StreamingPhase(remoteRootHash: String): Pair<HashMap<String, SignedBlock>, Set<String>> {
        val request = ProtocolMessageProto.ProtocolMessage.newBuilder()
                .setType(ProtocolMessageProto.ProtocolMessage.MessageType.START_STREAM)
                .setStreamRequest(ProtocolMessageProto.StreamRequest.newBuilder()
                        .setFilter(blockRepo.boomer.getFilter(blockRepo.getRootBlock().cryptoHash)!!.toProto())
                        .build()
                )
                .build()
                .toByteArray()
        byteStream.send(endpointId, request)
        Log.d("Checking", "Sent start streaming request")
        val s = streamChannel.take()!!
        Log.d("Checking", "Got stream")
        val received = HashMap<String, SignedBlock>()
        val exist = HashSet<String>()
        val need = HashSet<String>(listOf(remoteRootHash))
        while (true) {
            val lengthBuf = ByteArray(5)
            if (read(s, lengthBuf, 4) == -1) return Pair<HashMap<String, SignedBlock>, Set<String>>(received, need - exist)
            val length = ByteBuffer.wrap(lengthBuf).getInt(0)
            Log.d("Checking", "block length: " + length.toString())
            val blockBuf = ByteArray(length)
            if (read(s, blockBuf, length) == -1) throw UnexpectedTerminationException("Network exception detected")
            val block = SignedBlock.fromProto(ProtocolMessageProto.SignedBlock.parseFrom(blockBuf))
            received.put(block.cryptoHash, block)
            need.addAll(block.unsignedBlock.parentHashes)
            exist.add(block.cryptoHash)
        }
    }

    private fun FillingGapPhase(blocksToAddByCryptoHash: HashMap<String, SignedBlock>, backTier: Set<String>, rootHash: String): Boolean {
        var ret = backTier.contains(rootHash)
        val blocksToFetch = ArrayList<String>(backTier.filter { !blockRepo.containsBlock(it) })
        while (blocksToFetch.isNotEmpty()) {
            val blocks = getRemoteBlocks(blocksToFetch)
            blocksToFetch.clear()
            for (block in blocks)
                blocksToAddByCryptoHash.put(block.cryptoHash, block)
            for (block in blocks)
                for (parent in block.unsignedBlock.parentHashes)
                    if (!blockRepo.containsBlock(parent) && !blocksToAddByCryptoHash.containsKey(parent))
                        blocksToFetch.add(parent)
                    else if (parent == rootHash)
                        ret = true
        }
        return ret
    }

    override fun run() {
        val currentRootBlock = blockRepo.getRootBlock()
        try {
            val remoteRootBlock = getRemoteRootBlock()
            Log.d("Checking", "Got remote root block")
            if (remoteRootBlock.cryptoHash == currentRootBlock.cryptoHash) {
                mergeCleanup()
                return
            }
            val frontierHashes = ArrayList<String>()
            if (!blockRepo.containsBlock(remoteRootBlock.cryptoHash))
                frontierHashes.add(remoteRootBlock.cryptoHash)
            val (blocksToAddByCryptoHash, backTier) = StreamingPhase(remoteRootBlock.cryptoHash)
            Log.d("Checking", "Streaming phase ended")
            blocksToAddByCryptoHash.forEach({
                Log.d("Checking", "In stream phase received ${it.value} with cryptoHash ${it.key}")
            })
            val seenCurrentRoot = FillingGapPhase(blocksToAddByCryptoHash, backTier, currentRootBlock.cryptoHash)
            Log.d("Checking", "Filling gap phase ended")
            if (!seenCurrentRoot) frontierHashes.add(currentRootBlock.cryptoHash)
            val root = addProofOfWitnessBlocks(
                    blocksToAddByCryptoHash,
                    frontierHashes,
                    currentRootBlock,
                    remoteRootBlock
            )
            blocksToAddByCryptoHash.forEach({
                Log.d("Checking", "Will be adding ${it.value} with cryptoHash ${it.key}")
            })
            if (blockRepo.verifyBlocks(blocksToAddByCryptoHash))
                blockRepo.updateBlockChain(blocksToAddByCryptoHash.values.toList(), root)
            mergeCleanup()
        } catch (ute: Exception) {
            ute.printStackTrace()
        }
    }

    /**
     * Runs the core block-merging algorithm, performing block validation and processing while
     * doing so.
     */
    /*override fun run() {
        val currentRootBlock = blockRepo.getRootBlock()
        try {

            Log.d("Checking", "Retrieving Remote Root Block")
            val remoteRootBlock = getRemoteRootBlock()
            Log.d("Checking", "Retrieved Remote Root Block")
            if (remoteRootBlock.cryptoHash == currentRootBlock.cryptoHash) {
                mergeCleanup()
                return
            }
            Log.d("Checking", "Retrieving Remote Blocks To Add")
            val (blocksToAddByCryptoHash, frontierHashes) = getRemoteBlocksToAdd(
                    currentRootBlock,
                    remoteRootBlock
            )
            Log.d("Checking", "Adding Proof Of Witness Remote Blocks")
            val root = addProofOfWitnessBlocks(
                    blocksToAddByCryptoHash,
                    frontierHashes,
                    currentRootBlock,
                    remoteRootBlock
            )
            blocksToAddByCryptoHash.forEach({
                Log.d("Checking", "Will be adding ${it.value} with cryptoHash ${it.key}")
            })
            val blocksToAdd = blocksToAddByCryptoHash.values.toList()
            Log.d("Checking", "Verifying Blockchain")
            if (blockRepo.verifyBlocks(blocksToAddByCryptoHash)) {
                Log.d("Checking", "Updating Blockchain")
                blockRepo.updateBlockChain(blocksToAdd, root)
            }
            mergeCleanup()
            Log.d("Checking", "Sent Merge Complete")
        } catch (ute: Exception) {
        }
    }*/

    /** Cleanup function once merging terminates. */
    private fun mergeCleanup() {
        Log.d("Checking", "Cleaned up")
        val mergeCompleteMessage = ProtocolMessageProto.ProtocolMessage.newBuilder()
                .setType(ProtocolMessageProto.ProtocolMessage.MessageType.MERGE_COMPLETE)
                .setNoBody(true)
                .build()
                .toByteArray()
        completed = true
        byteStream.send(endpointId, mergeCompleteMessage)
    }

    /** Fetch the remote endpoint's root block. */
    private fun getRemoteRootBlock(): SignedBlock {
        val request = ProtocolMessageProto.ProtocolMessage.newBuilder()
                .setType(ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_ROOT_BLOCK_REQUEST)
                .setNoBody(true)
                .build()
                .toByteArray()
        byteStream.send(endpointId, request)
        while (true) {
            val response = responseChannel.take()
            if (response.type == ProtocolMessageProto.ProtocolMessage.MessageType.MERGE_INTERRUPTED) {
                throw UnexpectedTerminationException("Network exception detected")
            } else if (response.type ==
                    ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_ROOT_BLOCK_RESPONSE
            ) {
                if (response.getRemoteRootBlockResponse.failedToRetrieve) {
                    throw UnexpectedTerminationException("Could not retrieve remote root block")
                }
                return SignedBlock.fromProto(response.getRemoteRootBlockResponse.remoteRootBlock)
            }
        }
    }

    /** Fetch the remote endpoint's blocks given the blocks crypto hashes. */
    private fun getRemoteBlocks(cryptoHashes: List<String>): List<SignedBlock> {
        val request = ProtocolMessageProto.ProtocolMessage.newBuilder()
                .setType(ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_BLOCKS_REQUEST)
                .setGetRemoteBlocksRequest(
                        ProtocolMessageProto.GetRemoteBlocksRequest.newBuilder()
                                .addAllCryptoHashes(cryptoHashes)
                                .build()
                ).build()
                .toByteArray()

        byteStream.send(endpointId, request)
        while (true) {
            val response = responseChannel.take()
            if (response.type == ProtocolMessageProto.ProtocolMessage.MessageType.MERGE_INTERRUPTED) {
                throw UnexpectedTerminationException("Network exception detected")
            } else if (response.type ==
                    ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_BLOCKS_RESPONSE
            ) {
                if (response.getRemoteBlocksResponse.failedToRetrieve) {
                    throw UnexpectedTerminationException("Could not retrieve remote blocks")
                }
                val result=
                if (response.getRemoteBlocksResponse.inStream) {
                    val s = streamChannel.take()!!
                    val receivedBlocks=ArrayList<SignedBlock>()
                    while (true) {
                        val lengthBuf = ByteArray(5)
                        if (read(s, lengthBuf, 4) == -1) break
                        val length = ByteBuffer.wrap(lengthBuf).getInt(0)
                        Log.d("Checking", "block length: " + length.toString())
                        val blockBuf = ByteArray(length)
                        if (read(s, blockBuf, length) == -1) throw UnexpectedTerminationException("Network exception detected")
                        val block = SignedBlock.fromProto(ProtocolMessageProto.SignedBlock.parseFrom(blockBuf))
                        receivedBlocks.add(block)
                    }
                    receivedBlocks
                } else
                    response.getRemoteBlocksResponse.remoteBlocksList.map {
                        SignedBlock.fromProto(
                                it
                        )
                    }
                if (result.map { it.cryptoHash }.toSet() != cryptoHashes.toSet()) {
                    throw UnexpectedTerminationException("Retrieved incomplete or incorrect set of remote blocks")
                }
                return result
            }
        }
    }

    /** Fetch the remote endpoint's proof of witness block given the parent hashes. */
    private fun getRemoteProofOfWitnessBlock(parentHashes: List<String>): SignedBlock {
        val request = ProtocolMessageProto.ProtocolMessage.newBuilder()
                .setType(ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_PROOF_OF_WITNESS_BLOCK_REQUEST)
                .setGetRemoteProofOfWitnessBlockRequest(
                        ProtocolMessageProto.GetRemoteProofOfWitnessBlockRequest.newBuilder()
                                .addAllParentHashes(parentHashes)
                                .build()
                ).build()
                .toByteArray()

        byteStream.send(endpointId, request)
        while (true) {
            val response = responseChannel.take()
            if (response.type == ProtocolMessageProto.ProtocolMessage.MessageType.MERGE_INTERRUPTED) {
                throw UnexpectedTerminationException("Network exception detected")
            } else if (response.type ==
                    ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_PROOF_OF_WITNESS_BLOCK_RESPONSE
            ) {
                val result = SignedBlock.fromProto(response.getRemoteProofOfWitnessBlockResponse.remoteProofOfWitnessBlock)
                if (result.unsignedBlock.parentHashes.toSet() != parentHashes.toSet()) {
                    throw UnexpectedTerminationException("Retrieved incorrect remote proof of witness block")
                }
                return result
            }
        }
    }

    /** Fetch the remote endpoint's timestamp. */
    private fun getRemoteTimestamp(): Long {
        val request = ProtocolMessageProto.ProtocolMessage.newBuilder()
                .setType(ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_TIMESTAMP_REQUEST)
                .setNoBody(true)
                .build()
                .toByteArray()

        byteStream.send(endpointId, request)
        while (true) {
            val response = responseChannel.take()
            if (response.type == ProtocolMessageProto.ProtocolMessage.MessageType.MERGE_INTERRUPTED) {
                throw UnexpectedTerminationException("Network exception detected")
            } else if (response.type ==
                    ProtocolMessageProto.ProtocolMessage.MessageType.GET_REMOTE_TIMESTAMP_RESPONSE
            ) {
                return response.getRemoteTimestampResponse.remoteTimestamp
            }
        }
    }
}