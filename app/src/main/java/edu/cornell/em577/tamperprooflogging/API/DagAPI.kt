package edu.cornell.em577.tamperprooflogging.API

import android.content.Context
import android.content.res.Resources
import edu.cornell.em577.tamperprooflogging.data.exception.PermissionNotFoundException
import edu.cornell.em577.tamperprooflogging.data.model.SignedBlock
import edu.cornell.em577.tamperprooflogging.data.model.Transaction
import edu.cornell.em577.tamperprooflogging.data.model.UnsignedBlock
import edu.cornell.em577.tamperprooflogging.data.source.BlockRepository
import edu.cornell.em577.tamperprooflogging.data.source.UserDataRepository
import edu.cornell.em577.tamperprooflogging.util.SingletonHolder
import java.util.*


abstract class DagCallback {
    abstract fun OnNewBlockAdded(hash: String)
}

class vegvisir private constructor(private val env: Pair<Context, Resources>) {
    companion object :
            SingletonHolder<vegvisir, Pair<Context, Resources>>(::vegvisir) {
    }



    private val userRepo = UserDataRepository.getInstance(Pair(env.first, env.second))
    private val blockRepo = BlockRepository.getInstance(Pair(env.first, env.second))
    //private val userPassword = env.third

    fun get_content(cryptoHashes: Collection<String>): List<SignedBlock> {
        return blockRepo.getBlocks(cryptoHashes);
    }

    fun get_leader(): String {
        return blockRepo.getRootBlock().cryptoHash;
    }

    fun add_block(block: SignedBlock) {
        blockRepo.updateBlockChain(listOf<SignedBlock>(block), block)
    }

    fun generate_user_block(transactions: List<Transaction>, password: String, parents: List<String>):SignedBlock {
        val (userId, userLocation) = userRepo.loadUserMetaData()
        if (!userRepo.isActiveUser(userId)) {
            throw PermissionNotFoundException("User certificate has been revoked")
        }
        val unsignedBlockToAdd = UnsignedBlock(
                userId,
                Calendar.getInstance().timeInMillis,
                userLocation,
                parents,
                transactions
        )
        val privateKey = userRepo.loadUserPrivateKey(password)
        return SignedBlock(unsignedBlockToAdd, unsignedBlockToAdd.sign(privateKey))
    }

    fun add_listener(cb: DagCallback) {
        blockRepo.register(cb)
    }


}