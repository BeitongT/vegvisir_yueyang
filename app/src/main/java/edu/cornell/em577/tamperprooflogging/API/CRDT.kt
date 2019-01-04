package edu.cornell.em577.tamperprooflogging.API

abstract class Operation {
    abstract fun AsString(): String
}

abstract class CRDT {
    //abstract fun getID(): String
    abstract fun parsedFromString(str: String): Operation
    abstract fun applyOperation(op: Operation)
}

class handleOfCRDT constructor(private val ps: PubSub, val id: String, val crdt: CRDT) {
    fun doOperation(op : Operation){
        ps.publish(id,op.AsString())
    }

}
