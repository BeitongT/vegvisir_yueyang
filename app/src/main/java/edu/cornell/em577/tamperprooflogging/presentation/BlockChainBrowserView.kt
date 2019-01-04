package edu.cornell.em577.tamperprooflogging.presentation

import android.content.Context
import android.graphics.Canvas
import android.graphics.Color
import android.graphics.Paint
import android.graphics.Path
import android.graphics.Point
import android.util.AttributeSet
import android.view.View
import edu.cornell.em577.tamperprooflogging.data.model.SignedBlock
import edu.cornell.em577.tamperprooflogging.data.source.BlockRepository
import java.util.*
import kotlin.collections.ArrayList
import kotlin.math.pow

/** Custom view for drawing the blockchain. */
class BlockChainBrowserView(context: Context, attributeSet: AttributeSet) : View(context, attributeSet) {

    /** A node in the directed acyclic graph of blocks that is fixed on a canvas */
    private data class CanvasBlockNode(
            val canvasBlock: CanvasBlock,
            val parents: List<CanvasBlockNode>
    ) {
        data class CanvasBlock(val signedBlock: SignedBlock, val point: Point, var color: Int)
    }

    private val paint = Paint(Paint.ANTI_ALIAS_FLAG)
    private val edge = Path()
    private val leftArrowEdge = Path()
    private val rightArrowEdge = Path()
    private var frontierCanvasBlockNode: CanvasBlockNode = canvasBlocksToCanvasBlockNodes(blocksToCanvasBlocks())
    var xViewportOffset = 0
    var yViewportOffset = 0

    companion object {
        private const val NODE_RADIUS = 15
        private const val EDGE_WIDTH = 2
        private const val ARROW_LENGTH = 10
        private const val ARROW_DEGREE_OFFSET = 30
        private const val INTRA_LAYER_DISTANCE = 50
        private const val INTER_LAYER_DISTANCE = 100
        private const val BASE_X = 100
        private const val BASE_Y = 100
    }

    /** Maps blocks in the repository to canvas blocks. */
    private fun blocksToCanvasBlocks(): HashMap<String, CanvasBlockNode.CanvasBlock> {
        // added: color difference
        val defaultColor = Color.BLACK
        val blockRepository = BlockRepository.getInstance(Pair(context, resources))
        val rootBlock = blockRepository.getRootBlock()
        val canvasBlockByCryptoHash = HashMap<String, CanvasBlockNode.CanvasBlock>()
        var currentLayer = listOf(rootBlock)
        var currentLayerNum = 0
        while (currentLayer.isNotEmpty()) {
            (0 until currentLayer.size).forEach({
                canvasBlockByCryptoHash[currentLayer[it].cryptoHash] =
                        CanvasBlockNode.CanvasBlock(
                                currentLayer[it],
                                Point(
                                        BASE_X - xViewportOffset + currentLayerNum * INTER_LAYER_DISTANCE,
                                        BASE_Y - yViewportOffset + it * INTRA_LAYER_DISTANCE
                                ),
                                defaultColor
                        )
            })
            val currentLayerParentHashes = LinkedHashSet<String>()
            for (block in currentLayer) {
                currentLayerParentHashes.addAll(block.unsignedBlock.parentHashes)
            }
            currentLayer = blockRepository.getBlocks(currentLayerParentHashes)
            currentLayerNum += 1
        }
        val chains = blockRepository.spliter.getChains()
        val rnd = Random()
        for (i in 0..(chains.size - 1)) {
            val color = Color.argb(255, rnd.nextInt(256), rnd.nextInt(256), rnd.nextInt(256));
            for (hash in chains[i])
                canvasBlockByCryptoHash[hash]!!.color = color
        }
        return canvasBlockByCryptoHash
    }

    /** Maps specified canvas blocks to canvas signedBlock nodes */
    private fun canvasBlocksToCanvasBlockNodes(
            canvasBlockByCryptoHash: HashMap<String, CanvasBlockNode.CanvasBlock>
    ): CanvasBlockNode {
        val blockRepository = BlockRepository.getInstance(Pair(context, resources))
        val frontierBlock = blockRepository.getRootBlock()
        val visitedCanvasBlockNodeByCryptoHash = HashMap<String, CanvasBlockNode>()
        val stack = ArrayDeque<CanvasBlockNode.CanvasBlock>(listOf(canvasBlockByCryptoHash[frontierBlock.cryptoHash]))

        while (stack.isNotEmpty()) {
            val canvasRootBlock = stack.pop()
            val canvasBlocksToVisit = ArrayList<CanvasBlockNode.CanvasBlock>()
            for (parentHash in canvasRootBlock.signedBlock.unsignedBlock.parentHashes) {
                if (parentHash !in visitedCanvasBlockNodeByCryptoHash) {
                    canvasBlocksToVisit.add(canvasBlockByCryptoHash[parentHash]!!)
                }
            }
            if (canvasBlocksToVisit.isNotEmpty()) {
                stack.push(canvasRootBlock)
                canvasBlocksToVisit.forEach({ stack.push(it) })
            } else if (canvasRootBlock.signedBlock.cryptoHash !in visitedCanvasBlockNodeByCryptoHash) {
                val rootNode =
                        CanvasBlockNode(
                                canvasRootBlock,
                                canvasRootBlock.signedBlock.unsignedBlock.parentHashes.map {
                                    visitedCanvasBlockNodeByCryptoHash[it]!!
                                })
                visitedCanvasBlockNodeByCryptoHash[canvasRootBlock.signedBlock.cryptoHash] = rootNode

                if (canvasRootBlock.signedBlock.cryptoHash == frontierBlock.cryptoHash) {
                    return rootNode
                }
            }
        }
        throw RuntimeException("Malformed blockchain")
    }

    /**
     * Updates the canvas frontier node. Assumes that the blockchain provided is consistent and
     * forms an acyclic directed graph
     */
    private fun updateFrontierCanvasBlockNode() {
        frontierCanvasBlockNode = canvasBlocksToCanvasBlockNodes(blocksToCanvasBlocks())
    }

    private fun drawGraph(canvas: Canvas) {
        drawNode(canvas, frontierCanvasBlockNode.canvasBlock.point,frontierCanvasBlockNode.canvasBlock.color)
        val queue = ArrayDeque(listOf(frontierCanvasBlockNode))
        val cryptoHashesOfVisitedBlocks = HashSet(listOf(frontierCanvasBlockNode.canvasBlock.signedBlock.cryptoHash))
        while (queue.isNotEmpty()) {
            val currentNode = queue.pop()
            for (parentNode in currentNode.parents) {
                if (parentNode.canvasBlock.signedBlock.cryptoHash !in cryptoHashesOfVisitedBlocks) {
                    drawNode(canvas, parentNode.canvasBlock.point, parentNode.canvasBlock.color)
                    cryptoHashesOfVisitedBlocks.add(parentNode.canvasBlock.signedBlock.cryptoHash)
                    queue.addLast(parentNode)
                }
                drawEdge(canvas, currentNode.canvasBlock.point, parentNode.canvasBlock.point)
            }
        }
    }

    /** Draw the provided point on the provided canvas. */
    private fun drawNode(canvas: Canvas, point: Point, color: Int) {
        paint.reset()
        paint.style = Paint.Style.FILL
        paint.color = color
        canvas.drawCircle(point.x.toFloat(), point.y.toFloat(), NODE_RADIUS.toFloat(), paint)
    }

    /** Compute the distance between the provided points. */
    private fun getDistance(src: Point, dest: Point): Double {
        return Math.sqrt((src.x - dest.x).toDouble().pow(2) + (src.y - dest.y).toDouble().pow(2))
    }

    /** Draw an arrowed edge between the two provided points on the canvas.  */
    private fun drawEdge(canvas: Canvas, src: Point, dest: Point) {
        paint.reset()
        edge.moveTo(src.x.toFloat(), src.y.toFloat())
        edge.lineTo(dest.x.toFloat(), dest.y.toFloat())

        val distance = getDistance(src, dest)
        val xCoord = src.x + (dest.x.toFloat() - src.x) * (distance - NODE_RADIUS) / distance
        val yCoord = src.y + (dest.y.toFloat() - src.y) * (distance - NODE_RADIUS) / distance

        val arrowRadianOffset = Math.PI * (ARROW_DEGREE_OFFSET / 180.0)
        val positiveOffsetCos = Math.cos(arrowRadianOffset)
        val positiveOffsetSin = Math.sin(arrowRadianOffset)
        val negativeOffsetCos = Math.cos(-arrowRadianOffset)
        val negativeOffsetSin = Math.sin(-arrowRadianOffset)
        val arrowXCoord = (src.x.toFloat() - dest.x) * ARROW_LENGTH / distance
        val arrowYCoord = (src.y.toFloat() - dest.y) * ARROW_LENGTH / distance

        val positiveOffsetArrowXCoord = positiveOffsetCos * arrowXCoord - positiveOffsetSin * arrowYCoord
        val positiveOffsetArrowYCoord = positiveOffsetSin * arrowXCoord + positiveOffsetCos * arrowYCoord
        val negativeOffsetArrowXCoord = negativeOffsetCos * arrowXCoord - negativeOffsetSin * arrowYCoord
        val negativeOffsetArrowYCoord = negativeOffsetSin * arrowXCoord + negativeOffsetCos * arrowYCoord

        leftArrowEdge.moveTo(xCoord.toFloat(), yCoord.toFloat())
        leftArrowEdge.lineTo(
                (xCoord.toFloat() + positiveOffsetArrowXCoord).toFloat(),
                (yCoord + positiveOffsetArrowYCoord).toFloat()
        )

        rightArrowEdge.moveTo(xCoord.toFloat(), yCoord.toFloat())
        rightArrowEdge.lineTo(
                (xCoord.toFloat() + negativeOffsetArrowXCoord).toFloat(),
                (yCoord + negativeOffsetArrowYCoord).toFloat()
        )

        paint.strokeWidth = EDGE_WIDTH.toFloat()
        paint.style = Paint.Style.STROKE
        paint.color = Color.BLACK
        canvas.drawPath(edge, paint)
        canvas.drawPath(leftArrowEdge, paint)
        canvas.drawPath(rightArrowEdge, paint)
    }

    /** Draw on the blockchain on the provided canvas. */
    override fun onDraw(canvas: Canvas) {
        paint.reset()
        edge.reset()
        leftArrowEdge.reset()
        rightArrowEdge.reset()
        updateFrontierCanvasBlockNode()
        drawGraph(canvas)
    }

    /** Retrieve the canvas block. */
    fun getCanvasBlock(xCoord: Int, yCoord: Int): SignedBlock? {
        val visited = HashSet<String>()
        val stack = ArrayDeque<CanvasBlockNode>(listOf(frontierCanvasBlockNode))
        val clickedPoint = Point(xCoord, yCoord)
        while (stack.isNotEmpty()) {
            val current = stack.pop()
            if (getDistance(current.canvasBlock.point, clickedPoint) <= NODE_RADIUS) {
                return current.canvasBlock.signedBlock
            }
            for (parent in current.parents) {
                if (parent.canvasBlock.signedBlock.cryptoHash !in visited) {
                    visited.add(parent.canvasBlock.signedBlock.cryptoHash)
                    stack.push(parent)
                }
            }
        }
        return null
    }
}