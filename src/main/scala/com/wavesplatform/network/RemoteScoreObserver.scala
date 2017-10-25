package com.wavesplatform.network

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

import com.wavesplatform.state2.ByteStr
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel._
import scorex.transaction.History
import scorex.utils.ScorexLogging

import scala.concurrent.duration.FiniteDuration


@Sharable
class RemoteScoreObserver(scoreTtl: FiniteDuration, lastSignatures: => Seq[ByteStr], initialLocalScore: BigInt)
  extends ChannelDuplexHandler with ScorexLogging {

  private val pinnedChannel = new AtomicReference[(Channel, BigInt)]()

  @volatile
  private var localScore = initialLocalScore

  private val scores = new ConcurrentHashMap[Channel, BigInt]

  private def channelWithHighestScore: Option[(Channel, BigInt)] =
    Option(scores.reduceEntries(1000, (c1, c2) => if (c1.getValue > c2.getValue) c1 else c2))
      .map(e => e.getKey -> e.getValue)

  override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
    ctx.channel().closeFuture().addListener { channelFuture: ChannelFuture =>
      val closedChannel = channelFuture.channel()
      resetPinnedChannel(closedChannel, "channel was closed")
      Option(scores.remove(closedChannel)).foreach { removedScore =>
        log.debug(s"${id(ctx)} Closed, removing score $removedScore")
      }

      changePinnedChannel1(closedChannel, "switching to second best channel")
    }
  }

  override def write(ctx: ChannelHandlerContext, msg: AnyRef, promise: ChannelPromise): Unit = msg match {
    case LocalScoreChanged(newLocalScore) =>
      // unconditionally update local score value and propagate this message downstream
      localScore = newLocalScore
      ctx.writeAndFlush(msg, promise)
      refreshPinnedChannel("local score was updated")

    case _ => ctx.write(msg, promise)
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: AnyRef): Unit = msg match {
    case newScore: History.BlockchainScore =>
      ctx.executor().schedule(scoreTtl) {
        if (scores.remove(ctx.channel(), newScore)) {
          log.trace(s"${id(ctx)} Score expired, removing $newScore")
        }
      }

      val previousScore = scores.put(ctx.channel(), newScore)
      if (previousScore != newScore) {
        log.trace(s"${id(ctx)} ${pinnedChannelId}New score: $newScore")
      }

      refreshPinnedChannel("").foreach { case (newChannel, newBestScore) =>
        log.debug(s"${id(ctx)} ${pinnedChannelId}New high score $newBestScore > $localScore, requesting extension")
        newChannel.writeAndFlush(LoadBlockchainExtension(lastSignatures))
      }

    case ExtensionBlocks(blocks) if pinnedChannel.get()._1 == ctx.channel() =>
      if (blocks.isEmpty) resetPinnedChannel(ctx.channel(), "Blockchain is up to date")
      else {
        log.debug(s"${id(ctx)} ${pinnedChannelId}Receiving extension blocks ${formatBlocks(blocks)}")
        super.channelRead(ctx, msg)
      }

    case ExtensionBlocks(blocks) =>
      log.debug(s"${id(ctx)} ${pinnedChannelId}Received blocks ${formatBlocks(blocks)} from non-pinned channel")

    case _ => super.channelRead(ctx, msg)
  }

  private def pinnedChannelId = Option(pinnedChannel.get()).fold("")(ch => s"${id(ch, "pinned: ")} ")

  private def resetPinnedChannel(fromExpected: Channel, reason: String): Boolean = changePinnedChannel(fromExpected, null, reason)

  private def refreshPinnedChannel(reason: String): Option[(Channel, BigInt)] = {
    channelWithHighestScore.flatMap(changePinnedChannel(_, reason))
  }

  private def changePinnedChannel(to: (Channel, BigInt), reason: String): Option[(Channel, BigInt)] = {
    import to.{_2 => newScore}
    val r: (Channel, BigInt) = pinnedChannel.updateAndGet {
      case (_, currentScore) if newScore > currentScore && newScore > localScore => to
      case orig => orig
    }

    if (r == to) {
      val toId = Option(r._1).map(id(_))
      log.debug(s"A new pinned channel $toId has score $newScore: $reason")
      Some(r)
    } else {
      log.trace(s"Pinned channel was not changed")
      None
    }
  }

  private def changePinnedChannel1(fromExpectedChannel: Channel, reason: String): Boolean = {
    channelWithHighestScore.exists { x =>
      import x.{_2 => newScore}
      val r: (Channel, BigInt) = pinnedChannel.updateAndGet {
        case (`fromExpectedChannel`, _) => x
        case orig => orig
      }

      val changed = r == x
      if (changed) {
        val toId = Option(r._1).map(id(_))
        log.debug(s"A new pinned channel $toId has score $newScore: $reason")
      } else {
        log.trace(s"Pinned channel was not changed")
      }
      changed
    }
  }

}
