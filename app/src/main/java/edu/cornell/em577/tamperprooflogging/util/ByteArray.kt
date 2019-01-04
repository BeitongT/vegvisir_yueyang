package edu.cornell.em577.tamperprooflogging.util

import android.util.Log
import java.nio.ByteBuffer

private val HEX_CHARS = "0123456789ABCDEF".toCharArray()

/**
 * Converts the ByteArray to a hex String representation. Paired with String.hexStringToByteArray.
 */
fun ByteArray.toHex() : String{
    val result = StringBuffer()

    forEach {
        val octet = it.toInt()
        val firstIndex = (octet and 0xF0).ushr(4)
        val secondIndex = octet and 0x0F
        result.append(HEX_CHARS[firstIndex])
        result.append(HEX_CHARS[secondIndex])
    }

    return result.toString()
}

fun ByteArray.hashToLong():Long{
    val bb=ByteBuffer.allocate(8)
    var count=0
    var result:Long=0
    forEach {
        bb.put(it)
        count++
        if(count==8){
            val temp=bb.getLong(0)
            result=result xor temp
            bb.clear()
            count=0
        }
    }
    for(sth in 1..(8-count))
        bb.put(0.toByte())
    result =result xor bb.getLong(0)
    return result
}