package edu.cornell.em577.tamperprooflogging.presentation

import android.os.Bundle
import android.support.v7.app.AppCompatActivity
import android.widget.ArrayAdapter
import android.widget.ListView
import edu.cornell.em577.tamperprooflogging.R
import edu.cornell.em577.tamperprooflogging.data.model.Transaction
import edu.cornell.em577.tamperprooflogging.data.source.RecordRepository

/** Activity responsible for viewing all record requests made. */
class ViewRecordRequestActivity : AppCompatActivity() {

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_view_record_request)

        val recordListView = findViewById<ListView>(R.id.recordList)
        val recordRepo = RecordRepository.getInstance(Pair(applicationContext, resources))
        val adapter = ArrayAdapter<Transaction>(
            this@ViewRecordRequestActivity,
            R.layout.transaction_list_item,
            recordRepo.getAllRecordRequests())

        recordListView.adapter = adapter
    }
}