package edu.cornell.em577.tamperprooflogging.presentation

import android.content.Intent
import android.graphics.Color
import android.os.Bundle
import android.support.v7.app.AppCompatActivity
import android.text.SpannableString
import android.text.Spanned
import android.text.style.StrikethroughSpan
import android.util.Log
import android.view.View
import android.widget.ArrayAdapter
import android.widget.EditText
import android.widget.ListView
import android.widget.TextView
import edu.cornell.em577.tamperprooflogging.R
import edu.cornell.em577.tamperprooflogging.data.exception.PermissionNotFoundException
import edu.cornell.em577.tamperprooflogging.data.model.Transaction
import edu.cornell.em577.tamperprooflogging.data.source.BlockRepository
import org.jetbrains.anko.textColor
import java.util.LinkedHashSet

class ThingsBrowserActivity : AppCompatActivity() {

    private var AddList : ArrayList<SpannableString>?=null
    private var RemoveList : ArrayList<SpannableString>?=null
    private var AddAdapter: ArrayAdapter<SpannableString>? = null
    private var RemoveAdapter: ArrayAdapter<SpannableString>? = null
    private enum class op
    {
        Add,Remove
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_things_browser)
        val temp = getThings()
        AddList=ArrayList<SpannableString>()
        RemoveList=ArrayList<SpannableString>()
        for(str in temp.first)
            AddList?.add(SpannableString(str))
        for(str in temp.second) {
            val spstr=SpannableString(str)
            spstr.setSpan(StrikethroughSpan(),0,spstr.length, Spanned.SPAN_INCLUSIVE_EXCLUSIVE)
            RemoveList?.add(spstr)
        }
        AddAdapter = ArrayAdapter(
                this@ThingsBrowserActivity,
                android.R.layout.simple_list_item_1,
                AddList)
        RemoveAdapter = ArrayAdapter(
                this@ThingsBrowserActivity,
                android.R.layout.simple_list_item_1,
                RemoveList
        )
        val addlistView = findViewById<ListView>(R.id.addedThings)
        val removelistView = findViewById<ListView>(R.id.removedThings)
        addlistView.adapter = AddAdapter
        removelistView.adapter = RemoveAdapter
    }

    /**
     * Listener to create a signed block based on the user password and meta-data as well as the
     * user supplied transactions upon pressing the createBlockButton.
     */


    /** Listener to add a transaction to the UI upon pressing the addTransactionButton. */
    fun getThings(): Pair<ArrayList<String>, ArrayList<String>> {
        val blockRepository = BlockRepository.getInstance(Pair(applicationContext, resources))
        val rootBlock = blockRepository.getRootBlock()
        val temp = ArrayList<Pair<Long, Pair<op, String>>>()
        var now = listOf(rootBlock)
        var nowparenthash = LinkedHashSet<String>()
        while (now.isNotEmpty()) {
            for (block in now) {
                val stamp = block.unsignedBlock.timestamp
                for (tra in block.unsignedBlock.transactions) {
                    if(tra.type!=Transaction.TransactionType.RECORD_REQUEST) continue
                    val content = tra.content
                    //toxic implementation begin
                    if (content[0] == 'A') temp.add(Pair(stamp, Pair(op.Add, content.substring(4))))
                    else temp.add(Pair(stamp, Pair(op.Remove, content.substring(7))))
                    //toxic implementation end
                }
                nowparenthash.addAll(block.unsignedBlock.parentHashes)
            }
            now = blockRepository.getBlocks(nowparenthash)
            nowparenthash.clear()
        }
        temp.sortByDescending { it.first }
        val resAdd = ArrayList<String>()
        val resRemove = ArrayList<String>()
        for (record in temp) {
            if (record.second.second !in resRemove && record.second.second !in resAdd)
                if (record.second.first == op.Add)
                    resAdd.add(record.second.second)
                else if (record.second.first == op.Remove)
                    resRemove.add(record.second.second)
        }
        return Pair(resAdd, resRemove)
    }
}