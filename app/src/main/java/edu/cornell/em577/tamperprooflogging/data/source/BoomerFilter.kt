package edu.cornell.em577.tamperprooflogging.data.source

import android.util.Log
import com.vegvisir.data.ProtocolMessageProto
import edu.cornell.em577.tamperprooflogging.data.model.SignedBlock
import edu.cornell.em577.tamperprooflogging.util.hashToLong
import java.security.MessageDigest
import java.util.ArrayList

class LengthNotMatchException(override val message: String) : RuntimeException(message)

class BoomerFilter(private val radix: Int){

    // 2^radix should be the length of the Filter
    // Warning: radix should not be too large
    class Filter(private val radix: Int) {
        companion object {
            val wordLen = 64
            fun fromProto(protoFilter: ProtocolMessageProto.Filter): Filter {
                val ret = Filter(protoFilter.radix)
                for (i in 0..(ret.length - 1))
                    ret.value[i] = protoFilter.valueList[i]
                return ret
            }
        }

        private val length = 1 shl (radix - wordLen / 8)
        private val value = Array<Long>(length, { _: Int -> 0 })
        private val mod = ((1 shl radix) - 1).toLong()

        fun set(x: Long) {
            val y = (x and mod).toInt()
            val i = y ushr 8
            val j = y and ((1 shl 8) - 1)
            this.value[i] = this.value[i] or (1 shl j).toLong()
        }

        fun or(o: Filter) {
            if (this.length != o.length) throw LengthNotMatchException("this radix: ${this.length}, other radix: ${o.length}")
            for (i in 0..(length - 1))
                this.value[i] = this.value[i] or o.value[i]
        }

        // comparision <=
        fun isIn(o: Filter): Boolean {
            if (this.length != o.length) throw LengthNotMatchException("this radix: ${this.length}, other radix: ${o.length}")
            for (i in 0..(length - 1))
                if ((this.value[i] and o.value[i]) != this.value[i]) return false
            return true
        }

        fun toProto(): ProtocolMessageProto.Filter {
            return ProtocolMessageProto.Filter.newBuilder()
                    .setRadix(this.radix)
                    .addAllValue(this.value.toList())
                    .build()
        }
    }

    companion object {
        const val K = 5
        const val debugLevel=0
    }

    private val cryptoHash2Filter = HashMap<String, Filter>()

    // Maybe cache K hashes is better
    private val cachedBlock = HashMap<String, SignedBlock>()

    private val onWaitingBlockList = HashMap<String, ArrayList<String>>()
    private val waitingNum = HashMap<String, Int>()

    private fun computeHash(block: SignedBlock, salt: Int): Long {
        val digest = MessageDigest.getInstance("SHA-256")
        val uBlock = block.unsignedBlock
        digest.update(uBlock.userId.toByteArray())
        digest.update(uBlock.location.toByteArray())
        digest.update(uBlock.timestamp.toString().toByteArray())
        digest.update(uBlock.parentHashes.sorted().toString().toByteArray())
        digest.update(uBlock.transactions.map { it.toString() }.sorted().toString().toByteArray())
        digest.update(salt.toString().toByteArray())
        return digest.digest().hashToLong()
    }

    fun addBlock(block: SignedBlock) {
        val hash = block.cryptoHash
        if (cryptoHash2Filter.containsKey(hash) || waitingNum.containsKey(hash))
            return
        var count = 0
        for (parent in block.unsignedBlock.parentHashes)
            if (!cryptoHash2Filter.containsKey(parent)) {
                if (!onWaitingBlockList.containsKey(parent))
                    onWaitingBlockList[parent] = ArrayList<String>()
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

    // recursive implementation(bad)
    private fun insertBlock(block: SignedBlock) {
        val hash = block.cryptoHash
        var f = Filter(radix)
        for (parent in block.unsignedBlock.parentHashes) {
            f.or(cryptoHash2Filter[parent]!!)
        }
        for (k in 1..K)
            f.set(computeHash(block, k))
        cryptoHash2Filter[hash] = f
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

    fun getFilter(hash: String): Filter? {
        if(debugLevel==1) println(cryptoHash2Filter)
        return cryptoHash2Filter.get(hash)
    }

    fun testFilter(filter: Filter): Set<String> {
        return cryptoHash2Filter.filterValues { !it.isIn(filter) }.keys
    }

}