package edu.cornell.em577.tamperprooflogging.data.source

import edu.cornell.em577.tamperprooflogging.data.model.SignedBlock
import kotlin.math.max

// A naive chainspliter which uses much memory
class ChainSpliter {
    private val chains = HashMap<Int, ArrayList<String>>()
    private val chainLength = ArrayList<Int>(listOf(0))
    private val auxiliaryInformation = HashMap<String, HashMap<Int, Int>>()
    private var chainNum = 0

    // maybe hashes and parents only is better
    private val cachedBlock = HashMap<String, SignedBlock>()

    private val onWaitingBlockList = HashMap<String, java.util.ArrayList<String>>()
    private val waitingNum = HashMap<String, Int>()

    fun addBlock(block:SignedBlock){
        val hash = block.cryptoHash
        if (auxiliaryInformation.containsKey(hash) || waitingNum.containsKey(hash))
            return
        var count = 0
        for (parent in block.unsignedBlock.parentHashes)
            if (!auxiliaryInformation.containsKey(parent)) {
                if (!onWaitingBlockList.containsKey(parent))
                    onWaitingBlockList[parent] = java.util.ArrayList<String>()
                onWaitingBlockList[parent]!!.add(hash)
                count++
            }
        if (count > 0) {
            assert(!waitingNum.containsKey(hash))
            cachedBlock[hash] = block
            waitingNum[hash] = count
        } else
            insertBlock(block)
    }

    private fun insertBlock(block: SignedBlock) {
        // base case for genesis block
        if (block.unsignedBlock.parentHashes.size == 0) {
            auxiliaryInformation[block.cryptoHash] = HashMap<Int, Int>()
            return
        }
        val newInformation = HashMap<Int, Int>()
        for (parent in block.unsignedBlock.parentHashes)
            for ((k, v) in auxiliaryInformation[parent]!!)
                if (!newInformation.containsKey(k))
                    newInformation[k] = v
                else
                    newInformation[k] = max(newInformation[k]!!, v)
        val infoIterator=newInformation.iterator()
        for ((k, v) in infoIterator)
            if (v < chainLength[k]) infoIterator.remove()
        // no chain is available
        if (newInformation.size == 0) {
            chainNum++
            chains[chainNum] = ArrayList(listOf(block.cryptoHash))
            chainLength.add(1)
            newInformation[chainNum] = 1
        } else {
            // choose one chain to add the new block
            var maxv = 0
            var maxk = -1
            for ((k, v) in newInformation)
                if (v > maxv) {
                    maxk = k
                    maxv = v
                }
            chains[maxk]!!.add(block.cryptoHash)
            chainLength[maxk]++
            newInformation[maxk] = newInformation[maxk]!! + 1
        }
        auxiliaryInformation.put(block.cryptoHash,newInformation)

        val hash=block.cryptoHash
        if(!onWaitingBlockList.containsKey(hash))
            return
        for (waitingBlockHash in onWaitingBlockList[hash]!!) {
            waitingNum[waitingBlockHash] = waitingNum[waitingBlockHash]!! - 1
            if(waitingNum[waitingBlockHash]==0) {
                insertBlock(cachedBlock[waitingBlockHash]!!)
                cachedBlock.remove(waitingBlockHash)
                waitingNum.remove(waitingBlockHash)
            }
        }
        onWaitingBlockList.remove(hash)
    }

    fun getChains():ArrayList<ArrayList<String>>{
        return ArrayList(chains.values)
    }
}