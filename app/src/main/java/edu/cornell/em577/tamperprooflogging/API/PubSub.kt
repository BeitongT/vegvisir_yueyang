package edu.cornell.em577.tamperprooflogging.API

import android.telecom.Call
import edu.cornell.em577.tamperprooflogging.data.model.SignedBlock
import edu.cornell.em577.tamperprooflogging.data.model.Transaction
import edu.cornell.em577.tamperprooflogging.API.vegvisir

abstract class PSCallback() {
    abstract fun OnEvent(event: String)
}

class PubSub constructor(private val blockchain: vegvisir, private val password: String) {

    private val cache = ArrayList<Transaction>()



    private val CBs =HashMap<String,ArrayList<PSCallback>>()

    init{
        blockchain.add_listener(object : DagCallback(){
            override fun OnNewBlockAdded(hash: String) {
                val block=blockchain.get_content(listOf(hash))[0]
                for (t in block.unsignedBlock.transactions)
                {
                    if(t.type==Transaction.TransactionType.RAW)
                    {
                        val topic=t.content
                        val event=t.content
                        if(CBs.get(topic)==null) continue
                        for(cb in CBs.get(topic)!!)
                            cb.OnEvent(event)
                    }
                }
            }
        })
    }

    fun publish(topic: String, event: String) {
        cache.add(Transaction(Transaction.TransactionType.RAW, topic, event))
    }

    fun flush() {
        val Block = blockchain.generate_user_block(cache, password, listOf(blockchain.get_leader()))
        blockchain.add_block(Block)
        cache.clear()
    }

    fun subsribe(topic:String,cb:PSCallback)
    {
        if(!CBs.containsKey(topic))
            CBs.put(topic,ArrayList<PSCallback>())
        CBs.get(topic)!!.add(cb)
    }

//    fun subsribeWithCRDT(crdt : CRDT)
//    {
//        val topic=crdt.getID()
//        if(!CBs.containsKey(topic))
//            CBs.put(topic,ArrayList<Callback>())
//        CBs.get(topic)!!.add(object : PubSub.Callback(){
//            override fun OnEvent(event: String) {
//                TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
//            }
//        })
//    }
}