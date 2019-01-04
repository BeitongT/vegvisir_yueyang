package edu.cornell.em577.tamperprooflogging.presentation

import android.content.Intent
import android.graphics.Color
import android.os.Bundle
import android.support.v7.app.AppCompatActivity
import android.view.View
import android.widget.*
import edu.cornell.em577.tamperprooflogging.R
import edu.cornell.em577.tamperprooflogging.data.exception.PermissionNotFoundException
import edu.cornell.em577.tamperprooflogging.data.model.Transaction
import edu.cornell.em577.tamperprooflogging.data.source.BlockRepository
import org.jetbrains.anko.textColor

/** Activity responsible for adding blocks. */
class ThingsActivity : AppCompatActivity() {

    //private val transactionList = ArrayList<Pair<String, String>>()
    //private var transactionAdapter: ArrayAdapter<Pair<String, String>>? = null
    var userPassword: String? = null

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_add_remove_things)
        //setContentView(R.layout.activity_add_block)


        userPassword = intent.getStringExtra("UserPassword")
    }

    /**
     * Listener to create a signed block based on the user password and meta-data as well as the
     * user supplied transactions upon pressing the createBlockButton.
     */
    fun confirmButtonListener(view: View) {
        val blockRepo = BlockRepository.getInstance(Pair(applicationContext, resources))
        val group=findViewById<RadioGroup>(R.id.selector)
        val result=findViewById<TextView>(R.id.thingsResult)
        val choice=
                if(group.checkedRadioButtonId==R.id.radioButtonAdd){resources.getString(R.string.Add)}
                else{
                    if(group.checkedRadioButtonId==R.id.radioButtonRemove){resources.getString(R.string.Remove)}
                    else{null}
                }
        if(choice==null)
        {
            result.visibility=View.VISIBLE
            result.text=resources.getText(R.string.choose_action)
            result.textColor=Color.RED
            return;
        }
        val thing=findViewById<EditText>(R.id.enterThings).text.toString()
        val transactionsToAdd = ArrayList<Transaction>()
        transactionsToAdd.add(
            Transaction(
                    Transaction.TransactionType.RECORD_REQUEST,
                    choice.plus(" ").plus(thing),
                    resources.getString(R.string.empty)
            )
        )
        result.visibility = View.VISIBLE
        try {
            if (blockRepo.addUserBlock(transactionsToAdd, userPassword!!)) {
                result.text = resources.getText(R.string.successfully_added_block)
                result.textColor = Color.GREEN
            } else {
                result.text = resources.getText(R.string.merge_in_progress)
                result.textColor = Color.RED
            }
        } catch (e: PermissionNotFoundException) {
            val intent = Intent(this, LoginActivity::class.java)
            intent.addFlags(Intent.FLAG_ACTIVITY_CLEAR_TOP)
            startActivity(intent)
        }
    }

}