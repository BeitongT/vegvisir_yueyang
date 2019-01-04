package edu.cornell.em577.tamperprooflogging.simulation

import com.vegvisir.data.ProtocolMessageProto
import edu.cornell.em577.tamperprooflogging.data.exception.SignedBlockNotFoundException
import edu.cornell.em577.tamperprooflogging.data.model.SignedBlock
import edu.cornell.em577.tamperprooflogging.data.model.UnsignedBlock
import edu.cornell.em577.tamperprooflogging.data.model.Transaction
import edu.cornell.em577.tamperprooflogging.data.source.BoomerFilter
import edu.cornell.em577.tamperprooflogging.util.toHex
import edu.cornell.em577.tamperprooflogging.util.hexStringToByteArray
import java.security.KeyFactory
import java.security.KeyPairGenerator
import java.security.MessageDigest
import java.security.PrivateKey
import java.security.SecureRandom
import java.security.spec.PKCS8EncodedKeySpec
import java.security.spec.X509EncodedKeySpec
import java.util.*
import javax.crypto.Cipher
import javax.crypto.SecretKeyFactory
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.PBEKeySpec
import javax.crypto.spec.SecretKeySpec

class blockRepository(val userRepo: userRepository) {
    companion object {
        const val BoomerRadix = 8
        const val ROOT = "Root"
        fun getG(): SignedBlock {
            return SignedBlock(UnsignedBlock("admin", 0L, "Somewhere", listOf(), listOf()), "admin")
        }
    }

    val signedBlockByCryptoHash = HashMap<String, SignedBlock>()
    val boomer = BoomerFilter(BoomerRadix)
    val isUpdating = false



    fun bootstrap(G: SignedBlock) {
        updateBlockChain(listOf(G), G)
    }

    fun updateBlockChain(signedBlocksToAdd: List<SignedBlock>, rootSignedBlock: SignedBlock) {
        signedBlocksToAdd.forEach { addBlock(it) }
        signedBlockByCryptoHash[ROOT] = rootSignedBlock
    }

    fun containsBlock(cryptoHash: String): Boolean {
        return signedBlockByCryptoHash.containsKey(cryptoHash)
    }

    private fun addBlock(block: SignedBlock) {
        signedBlockByCryptoHash[block.cryptoHash] = block
        boomer.addBlock(block)
    }


    fun getRootBlock(): SignedBlock {
        return signedBlockByCryptoHash.getOrElse(ROOT, {
            throw SignedBlockNotFoundException(
                    "Root block was not initialized in the blockchain"
            )
        })
    }

    fun getBlocks(cryptoHashes: Collection<String>): List<SignedBlock> {
        return cryptoHashes.map {
            signedBlockByCryptoHash.getOrElse(it, {
                throw SignedBlockNotFoundException(
                        "The repository does not hold a signed block with a cryptographic hash $it"
                )
            })
        }
    }

    fun addUserBlock(transactions: List<Transaction>, password: String) {
        if (!isUpdating) {
            val (userId, userLocation) = userRepo.loadUserMetaData()
            val unsignedBlockToAdd = UnsignedBlock(
                    userId,
                    Calendar.getInstance().timeInMillis,
                    userLocation,
                    listOf(signedBlockByCryptoHash[ROOT]!!.cryptoHash),
                    transactions
            )
            val privateKey = userRepo.loadUserPrivateKey(password)
            val signedBlockToAdd =
                    SignedBlock(unsignedBlockToAdd, unsignedBlockToAdd.sign(privateKey))
            updateBlockChain(listOf(signedBlockToAdd), signedBlockToAdd)
        }
    }
}

class userRepository(val userId: String, val userLocation: String, val userPassword: String) {
    companion object {
        private const val ADMIN_NAME = "Certificate Authority"
        private const val ADMIN_LOCATION = "Origin"

        private const val USER = "User"
        private const val USER_ID = "userId"
        private const val USER_LOCATION = "userLocation"
        private const val USER_HASHED_PASS = "userHashedPassword"
        private const val USER_SALT = "userSalt"
        private const val USER_PUBLIC_KEY = "userPublicKey"
        private const val USER_PRIVATE_KEY="userPrivateKey"
        private const val USER_SYM_KEY_IV = "userSymKeyIv"
        private const val ENC_USER_PRIVATE_KEY = "encUserPrivateKey"
        private const val SIG_KEYGEN_ALGO = "RSA"
        private const val ENC_KEYGEN_ALGO = "PBKDF2withHmacSHA1"
        private const val ENC_ALGO = "AES/CBC/PKCS5Padding"
        private const val BASE_ENC_ALGO = "AES"
        private const val SALT_LEN = 8
        private const val ITERATION_COUNT = 6
        private const val KEY_LEN = 128

        private fun getSecretSpec(password: String, salt: ByteArray): SecretKeySpec {
            val factory = SecretKeyFactory.getInstance(ENC_KEYGEN_ALGO)
            val spec = PBEKeySpec(password.toCharArray(), salt, ITERATION_COUNT, KEY_LEN)
            return SecretKeySpec(factory.generateSecret(spec).encoded, BASE_ENC_ALGO)
        }
    }

    val userProperties =registerUser()

    fun registerUser(): HashMap<String, Any> {
        val (properties, salt) = mapUserAuthData(userId, userLocation, userPassword)
        val (publicKeyEncoded, privateKeyEncoded) = generateEncodedRSAKeyPair()
        properties[USER_PUBLIC_KEY] = publicKeyEncoded.toHex()
        val (encPrivateKey, iv) = encryptBytes(userPassword, salt, privateKeyEncoded)
        properties[USER_SYM_KEY_IV] = iv.toHex()
        properties[ENC_USER_PRIVATE_KEY] = encPrivateKey.toHex()
        return properties
    }

    fun loadUserMetaData(): Pair<String, String> {
        val properties = userProperties
        val userId = properties[USER_ID] as String
        val userLocation = properties[USER_LOCATION] as String
        return Pair(userId, userLocation)
    }

    fun loadUserPrivateKey(password: String): PrivateKey {
        val properties = userProperties
        val salt = (properties[USER_SALT] as String).hexStringToByteArray()
        val secret = getSecretSpec(password, salt)
        val cipher = Cipher.getInstance(ENC_ALGO)
        val iv = (properties[USER_SYM_KEY_IV] as String).hexStringToByteArray()
        cipher.init(Cipher.DECRYPT_MODE, secret, IvParameterSpec(iv))
        val cipherText = (properties[ENC_USER_PRIVATE_KEY] as String).hexStringToByteArray()
        val pkcs8EncodedKeySpec = PKCS8EncodedKeySpec(cipher.doFinal(cipherText))
        val keyFactory = KeyFactory.getInstance(SIG_KEYGEN_ALGO)
        return keyFactory.generatePrivate(pkcs8EncodedKeySpec)
    }



    fun loadUserHexPublicKey(): String {
        val properties = userProperties
        return properties[USER_PUBLIC_KEY] as String
    }

    private fun mapUserAuthData(userId: String, userLocation: String, userPassword: String): Pair<HashMap<String, Any>, ByteArray> {
        val properties = HashMap<String, Any>()
        properties[USER_ID] = userId
        properties[USER_LOCATION] = userLocation

        val salt = ByteArray(SALT_LEN)
        SecureRandom().nextBytes(salt)
        properties[USER_SALT] = salt.toHex()

        val digest = MessageDigest.getInstance("SHA-256")
        digest.update(userPassword.toByteArray())
        digest.update(salt)
        properties[USER_HASHED_PASS] = digest.digest().toHex()
        return Pair(properties, salt)
    }

    private fun generateEncodedRSAKeyPair(): Pair<ByteArray, ByteArray> {
        val keyGen = KeyPairGenerator.getInstance(SIG_KEYGEN_ALGO)
        keyGen.initialize(4096)
        val keyPair = keyGen.genKeyPair()
        val privateKey = keyPair.private
        val publicKey = keyPair.public
        val x509EncodedKeySpec = X509EncodedKeySpec(publicKey.encoded)
        val pkcs8EncodedKeySpec = PKCS8EncodedKeySpec(privateKey.encoded)
        return Pair(x509EncodedKeySpec.encoded, pkcs8EncodedKeySpec.encoded)
    }

    private fun encryptBytes(password: String, salt: ByteArray, bytes: ByteArray): Pair<ByteArray, ByteArray> {
        val secret = getSecretSpec(password, salt)
        val cipher = Cipher.getInstance(ENC_ALGO)
        cipher.init(Cipher.ENCRYPT_MODE, secret)
        val iv = cipher.parameters.getParameterSpec(IvParameterSpec::class.java).iv
        val cipherText = cipher.doFinal(bytes)
        return Pair(cipherText, iv)
    }


}

class Protocol(
        private val sim: Simulator,
        private val userId: String,
        private val userLocation: String,
        private val userPassword: String,
        private val G: SignedBlock
) {
    companion object {
        const private val default="simple"
        const val simple="simple"
        const val bloomfilter="bloomFilter"
        var protocolType:String= default
        fun changeProtocol(name:String){
            protocolType=name
        }
    }
    private val blockRepo = blockRepository(userRepository(userId, userLocation, userPassword))
    private val channel4Network = sim.createChannel<Pair<Channel<Message>,networkChannel>>()
    private val establishedConnectionIn = channel4Network.first
    private val establishedConnectionOut = channel4Network.second
    private var connectedID: String? = null
    private var connection: networkChannel? = null
    private var wrappedChannel:Channel<Message>?=null

    init {
        blockRepo.bootstrap(G)
        val mergeProtocol = EstablishRemoteExchangeProtocol(sim, userId, userPassword, establishedConnectionIn, blockRepo)
        mergeProtocol.start()
    }

    fun equals(p:Protocol):Boolean{
        return blockRepo.signedBlockByCryptoHash.equals(p.blockRepo.signedBlockByCryptoHash)
    }

    fun onConnectionBuild(endpointId: String, conn: networkChannel) {
        if (connectedID == null) {
            connectedID = endpointId
            connection = conn
            val (ch0,ch1)=sim.createChannel<Message>()
            connection!!.onReceive(::wrappedReceive,arrayOf(connection!!,ch0))
            wrappedChannel=ch0
            establishedConnectionOut.send(Pair(ch1,connection!!))
        } else {
            conn.close()
        }
    }

    private fun wrappedReceive(m:Message,args:argsType){
        args!!
        val conn=args[0] as networkChannel
        val ch=args[1] as Channel<Message>
        ch.send(m)
        conn.onReceive(::wrappedReceive,arrayOf(conn,ch))
    }

    fun onDisconnected(endpointId: String) {
        if (endpointId == connectedID) {
            val mergeInterruptedMessage = ProtocolMessageProto.ProtocolMessage.newBuilder()
                    .setType(ProtocolMessageProto.ProtocolMessage.MessageType.MERGE_INTERRUPTED)
                    .setNoBody(true)
                    .build()
            wrappedChannel!!.send(Message(mergeInterruptedMessage))
            connectedID = null
        }
    }

    fun onTransactionAdded(transaction: Transaction) {
        blockRepo.addUserBlock(listOf(transaction), userPassword)
    }
}