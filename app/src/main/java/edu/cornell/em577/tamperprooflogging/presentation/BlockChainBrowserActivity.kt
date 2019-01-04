package edu.cornell.em577.tamperprooflogging.presentation

import android.content.Context
import android.content.Intent
import android.os.Bundle
import android.support.v4.content.ContextCompat.startActivity
import android.support.v4.view.GestureDetectorCompat
import android.support.v7.app.AppCompatActivity
import android.view.GestureDetector
import android.view.MotionEvent
import edu.cornell.em577.tamperprooflogging.R

/** Activity for presenting the blockchain to the user. */
class BlockChainBrowserActivity : AppCompatActivity() {

    private var detector: GestureDetectorCompat? = null
    var browserView: BlockChainBrowserView? = null

    /** Listens for user gestures on the canvas. */
    class BlockChainBrowserGestureListener(
        private val context: Context,
        private val bundle: Bundle?,
        private val activity: BlockChainBrowserActivity
    ) :
        GestureDetector.SimpleOnGestureListener() {
        override fun onScroll(e1: MotionEvent?, e2: MotionEvent?, distanceX: Float, distanceY: Float): Boolean {
            activity.browserView?.xViewportOffset = activity.browserView!!.xViewportOffset + distanceX.toInt()
            activity.browserView?.yViewportOffset = activity.browserView!!.yViewportOffset + distanceY.toInt()
            activity.browserView?.invalidate()
            return false
        }

        override fun onSingleTapUp(e: MotionEvent?): Boolean {
            val blockTapped = activity.browserView?.getCanvasBlock(e?.x!!.toInt(), e.y.toInt())
            if (blockTapped != null) {
                val intent = Intent(activity, ViewBlockActivity::class.java)
                intent.putExtra("SignedBlock", blockTapped.toProto().toByteArray())
                intent.flags = Intent.FLAG_ACTIVITY_NEW_TASK
                startActivity(context, intent, bundle)
            }
            return false
        }
    }
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_block_chain_browser)
        browserView = findViewById(R.id.blockChainBrowserView)
        detector = GestureDetectorCompat(
            this, BlockChainBrowserGestureListener(applicationContext, savedInstanceState,this)
        )
    }

    /** Shows the contents of the block that was tapped in another activity. */
    override fun onTouchEvent(event: MotionEvent?): Boolean {
        detector?.onTouchEvent(event)
        return super.onTouchEvent(event)
    }
}
