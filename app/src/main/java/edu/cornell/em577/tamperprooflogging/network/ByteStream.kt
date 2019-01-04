package edu.cornell.em577.tamperprooflogging.network

import android.content.Context
import android.os.Handler
import android.util.Log
import com.google.android.gms.nearby.Nearby
import com.google.android.gms.nearby.connection.AdvertisingOptions
import com.google.android.gms.nearby.connection.ConnectionInfo
import com.google.android.gms.nearby.connection.ConnectionLifecycleCallback
import com.google.android.gms.nearby.connection.ConnectionResolution
import com.google.android.gms.nearby.connection.ConnectionsClient
import com.google.android.gms.nearby.connection.DiscoveredEndpointInfo
import com.google.android.gms.nearby.connection.DiscoveryOptions
import com.google.android.gms.nearby.connection.EndpointDiscoveryCallback
import com.google.android.gms.nearby.connection.Payload
import com.google.android.gms.nearby.connection.PayloadCallback
import com.google.android.gms.nearby.connection.PayloadTransferUpdate
import com.google.android.gms.nearby.connection.Strategy
import com.vegvisir.data.ProtocolMessageProto
import edu.cornell.em577.tamperprooflogging.util.SingletonHolder
import java.io.File
import java.io.InputStream
import java.util.concurrent.LinkedBlockingQueue

/**
 * Thread-safe reliable bidirectional byte stream between two network endpoints used to send and
 * receive data over the network, where each local send call is paired with a corresponding
 * remote receive call.
 */
class ByteStream private constructor(private val env: Pair<Context, String>) {

    companion object :
            SingletonHolder<ByteStream, Pair<Context, String>>(::ByteStream) {

        /**
         * Returns the service id. This represents the action this connection is for. When discovering,
         * we'll verify that the advertiser has the same service id before we consider connecting to them.
         */
        private const val serviceId = "vegvisir"

        /**
         * Returns the strategy we use to connect to other devices. Only devices using the same strategy
         * and service id will appear when discovering. Strategies determine how many incoming and outgoing
         * connections are possible at the same time, as well as how much bandwidth is available for use.
         */
        private val strategy = Strategy.P2P_STAR

        /** Represents a device we can talk to. */
        private data class Endpoint constructor(val id: String)

        /** Connection states. */
        private enum class State {
            SEARCHING,
            CONNECTED
        }
    }

    val userId = env.second

    /** Our handler to Nearby Connections. Shared object between UI thread and background threads */
    private val mConnectionsClient: ConnectionsClient = Nearby.getConnectionsClient(env.first)

    /** The device we are currently connected to. */
    private val mEstablishedConnection = LinkedBlockingQueue<Endpoint>(1)

    /**
     * Identifier of the device we are currently in connection with. Null if we are not connected to
     * any device.
     */
    @Volatile
    private var mEndpointId: Endpoint? = null

    /** Current state of the connection. */
    private var mState = State.SEARCHING

    /** Buffer of received byte arrays. */
    private val mRecvBuffer = LinkedBlockingQueue<ByteArray>()

    // Buffer of received InputStream
    private val mStreamBuffer = LinkedBlockingQueue<InputStream>()

    // Buffer of receive file
    private val mFileBuffer = LinkedBlockingQueue<File>()

    private val incomingFilePayload = HashMap<Long, Payload>()

    private val outgoingTempFilePayLoad = HashMap<Long, Pair<Payload, File>>()

    /** Callbacks for connections to other devices. */
    private val mConnectionLifecycleCallback = object : ConnectionLifecycleCallback() {

        override fun onConnectionInitiated(endpointId: String, connectionInfo: ConnectionInfo) {
            if (mEndpointId == null) {
                synchronized(mConnectionsClient) {
                    if(mEndpointId==null)
                        mConnectionsClient.acceptConnection(endpointId, mPayloadCallback)
                    else mConnectionsClient.rejectConnection(endpointId)
                }
            } else {
                synchronized(mConnectionsClient) {
                    mConnectionsClient.rejectConnection(endpointId)
                }
            }
        }

        override fun onConnectionResult(endpointId: String, result: ConnectionResolution) {
            if (mEndpointId == null) {
                if (result.status.isSuccess) {
                    setState(State.CONNECTED)
                    mEndpointId = Endpoint(endpointId)
                    Log.d("Checking", "Connected "+endpointId)
                    mEstablishedConnection.put(mEndpointId)
                } else {
                    setState(State.SEARCHING)
                }
            }
        }

        override fun onDisconnected(endpointId: String) {
            Log.d("Checking", "Lost "+endpointId)
            if (endpointId == mEndpointId?.id) {
                setState(State.SEARCHING)
                val mergeInterruptedMessage = ProtocolMessageProto.ProtocolMessage.newBuilder()
                        .setType(ProtocolMessageProto.ProtocolMessage.MessageType.MERGE_INTERRUPTED)
                        .setNoBody(true)
                        .build()
                        .toByteArray()
                mRecvBuffer.put(mergeInterruptedMessage)
            }
        }
    }

    /** Callbacks for payloads (bytes of data) sent from another device to us.  */
    private val mPayloadCallback = object : PayloadCallback() {
        override fun onPayloadReceived(endpointId: String, payload: Payload) {
            if (endpointId == mEndpointId?.id) {
                if (payload.type == Payload.Type.BYTES) {
                    mRecvBuffer.put(payload.asBytes())
                } else if (payload.type == Payload.Type.STREAM) {
                    mRecvBuffer.put(ProtocolMessageProto.ProtocolMessage.newBuilder()
                            .setType(ProtocolMessageProto.ProtocolMessage.MessageType.GET_STREAM)
                            .setNoBody(true)
                            .build()
                            .toByteArray())
                    mStreamBuffer.put(payload.asStream()!!.asInputStream())
                } else if (payload.type == Payload.Type.FILE) {
                    incomingFilePayload.put(payload.id, payload)
                }
            } else {
                synchronized(mConnectionsClient) {
                    mConnectionsClient.disconnectFromEndpoint(endpointId)
                }
            }
        }

        override fun onPayloadTransferUpdate(endpointId: String, update: PayloadTransferUpdate) {
            when (update.status) {
                PayloadTransferUpdate.Status.IN_PROGRESS ->
                    return
                PayloadTransferUpdate.Status.SUCCESS -> {
                    if (incomingFilePayload.containsKey(update.payloadId)) {
                        val payload = incomingFilePayload.remove(update.payloadId)!!
                        when (payload.type) {
                            Payload.Type.FILE -> {
                                Log.d("Checking","Received File")
                                val file = payload.asFile()!!.asJavaFile()
                                mFileBuffer.put(file)
                                mRecvBuffer.put(ProtocolMessageProto.ProtocolMessage.newBuilder()
                                        .setType(ProtocolMessageProto.ProtocolMessage.MessageType.GET_FILE)
                                        .setNoBody(true)
                                        .build()
                                        .toByteArray())
                            }
                            else -> {
                            }
                        }
                    } else if (outgoingTempFilePayLoad.containsKey(update.payloadId)) {
                        val (payload, file) = outgoingTempFilePayLoad.remove(update.payloadId)!!
                        when (payload.type) {
                            Payload.Type.FILE -> {
                                Log.d("Checking","Sent File")
                                file.deleteOnExit()
                            }
                            else -> {
                            }
                        }
                    }
                }
            }
        }
    }


    /** Sets the device to discovery mode. It will now listen for devices in advertising mode. */
    private fun startDiscovering() {
        mConnectionsClient
                .startDiscovery(
                        serviceId,
                        object : EndpointDiscoveryCallback() {
                            override fun onEndpointFound(endpointId: String, info: DiscoveredEndpointInfo) {
                                Log.d("Checking", "Found " + endpointId)
                                if (serviceId == info.serviceId) {
                                    Log.d("Checking", "Found friend")
                                    val endpoint = Endpoint(endpointId)
                                    synchronized(mConnectionsClient) {
                                        mConnectionsClient.stopDiscovery()
                                        mConnectionsClient.requestConnection(userId, endpoint.id, mConnectionLifecycleCallback)
                                    }
                                }
                            }

                            override fun onEndpointLost(endpointId: String) {}
                        },
                        DiscoveryOptions.Builder().setStrategy(strategy).build()
                )
    }

    /** Sets the device to advertising mode. It will broadcast to other devices in discovery mode. */
    private fun startAdvertising() {
        mConnectionsClient
                .startAdvertising(
                        userId,
                        serviceId,
                        mConnectionLifecycleCallback,
                        AdvertisingOptions.Builder().setStrategy(strategy).build()
                )
    }

    /** Resets and clears all state in Nearby Connections.  */
    private fun stopAllEndpoints() {
        mEndpointId = null
        mEstablishedConnection.clear()
        synchronized(mConnectionsClient) {
            mConnectionsClient.stopAllEndpoints()
        }
    }

    /**
     * Sets the state of the underlying connection of the byte stream, and trigger any
     * necessary changes to Google Nearby modes on state changes.
     */
    private fun setState(state: State) {
        mState = state

        when (state) {
            State.SEARCHING -> {
                Log.d("Checking","Searching")
                stopAllEndpoints()
                startDiscovering()
                startAdvertising()
            }
            State.CONNECTED -> {
                synchronized(mConnectionsClient) {
                    mConnectionsClient.stopDiscovery()
                    mConnectionsClient.stopAdvertising()
                }
            }
        }
    }

    /** Enter advertising and listening mode simultaneously. */
    fun create() {
        setState(State.SEARCHING)
    }

    /** Block until a connection has been established and returns the endpointId. */
    fun establishConnection(): String {
        Log.d("Checking", "-1")
        mRecvBuffer.clear()
        mStreamBuffer.clear()
        return mEstablishedConnection.take().id
    }

    /** Send the provided byte array over the link. Paired with a corresponding remote recv call. */
    fun send(endpointId: String, byteArray: ByteArray) {
        synchronized(mConnectionsClient) {
            mConnectionsClient.sendPayload(endpointId, Payload.fromBytes(byteArray))
        }
    }

    fun sendStream(endpointId: String,inS:InputStream){
        synchronized(mConnectionsClient){
            mConnectionsClient.sendPayload(endpointId, Payload.fromStream(inS))
        }
    }

    fun sendTempFile(endpointId: String, file: File) {
        synchronized(mConnectionsClient) {
            val payload = Payload.fromFile(file)
            mConnectionsClient.sendPayload(endpointId, payload)
            outgoingTempFilePayLoad.put(payload.id, Pair(payload, file))
        }
    }

    /** Blocking call that returns the byte array sent by the corresponding remote send call. */
    fun recv(): ByteArray {
        return mRecvBuffer.take()
    }

    fun recvStream(): InputStream {
        return mStreamBuffer.take()
    }

    fun recvFile(): File {
        return mFileBuffer.take()
    }

    fun close() {
        val mainHandler = Handler(env.first.mainLooper)
        mainHandler.post({
            setState(State.SEARCHING)
        })
    }
}