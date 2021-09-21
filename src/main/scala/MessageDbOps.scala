package messagedb

import cats.syntax.all._
import cats.effect._
import fs2.{Stream, Pipe}
import scala.concurrent.duration.FiniteDuration
import io.circe._
import io.circe.syntax._
import io.circe.generic.semiauto._
import java.util.UUID

object MessageDbOps {

  case class PollingState(
    nextPositionToGet: Long,
    lastPositionGot: Long,
  ) {
    def update(next: Long): PollingState = 
      PollingState(
        nextPositionToGet = next,
        lastPositionGot = this.nextPositionToGet
      )
    def gettingSamePosition: Boolean = 
      nextPositionToGet == lastPositionGot
    def sleepIfGettingSamePosition[F[_]: Temporal](d: FiniteDuration): Stream[F, Nothing] =
      if (gettingSamePosition)
        Stream.sleep_(d)
      else
        Stream.empty
  }

  object PollingState {
    def apply(next: Long): PollingState = 
      PollingState(
        nextPositionToGet = next,
        lastPositionGot = Long.MinValue
      )
  }

  case class SubscriberState(
    position: Long,
  )

  object SubscriberState {
    val Category = "subscriberPosition"
    def streamName(subscriberId: String): String = s"$Category-$subscriberId"
    val MessageType = "Read"
    val Default = SubscriberState(0L)
    implicit val encoder: Encoder[SubscriberState] = deriveEncoder[SubscriberState]
    implicit val decoder: Decoder[SubscriberState] = deriveDecoder[SubscriberState]
  }

}

case class MessageDbOps[F[_]: Temporal](messageDb: MessageDb[F]) {
  import MessageDbOps._

  def getMessagesUnbounded(
    getMessages: Long => Stream[F, MessageDb.Read.Message],
    getPosition: MessageDb.Read.Message => Long,
    position: Long,
    tickInterval: FiniteDuration,
  ): Stream[F, MessageDb.Read.Message] = 
    Stream.eval(Ref.of[F, PollingState](PollingState(position))).flatMap(state =>
      Stream.repeatEval(
        state.get.map(s => 
          s.sleepIfGettingSamePosition[F](tickInterval) ++
          getMessages(s.nextPositionToGet)
        )
      ).flatten.evalTap(m => 
        state.update(_.update(getPosition(m) + 1))
      )
    )

  def getStreamMessagesUnbounded(
    streamName: String,
    position: Long,
    batchSize: Option[Long],
    condition: Option[String],
    tickInterval: FiniteDuration,
  ): Stream[F, MessageDb.Read.Message] = 
    getMessagesUnbounded(
      p => messageDb.getStreamMessages(streamName, p.some, batchSize, condition),
      _.position,
      position,
      tickInterval,
    )

  def getCategoryMessagesUnbounded(
    category: String,
    position: Long,
    batchSize: Option[Long],
    correlation: Option[String],
    consumerGroupMember: Option[Long],
    consumerGroupSize: Option[Long],
    condition: Option[String],
    tickInterval: FiniteDuration,
  ): Stream[F, MessageDb.Read.Message] = 
    getMessagesUnbounded(
      p => messageDb.getCategoryMessages(category, p.some, batchSize, correlation, consumerGroupMember, consumerGroupSize, condition),
      _.globalPosition,
      position,
      tickInterval,
    )

  def loadPosition(subscriberId: String): F[Long] = 
    for {
      o <- messageDb.getLastStreamMessage(SubscriberState.streamName(subscriberId))
      s <- o.fold(SubscriberState.Default.pure[F])(_.decodeData[SubscriberState].liftTo[F])
    } yield s.position

  def getCategoryMessagesUnbounded(
    category: String,
    subscriberId: String,
    batchSize: Option[Long],
    correlation: Option[String],
    consumerGroupMember: Option[Long],
    consumerGroupSize: Option[Long],
    condition: Option[String],
    tickInterval: FiniteDuration,
  ): Stream[F, MessageDb.Read.Message] = 
    Stream.eval(loadPosition(subscriberId)).flatMap(p => 
      getCategoryMessagesUnbounded(category, p, batchSize, correlation, consumerGroupMember, consumerGroupSize, condition, tickInterval)
    )

  def writePosition(subscriberId: String, position: Long): F[Unit] = 
    messageDb.writeMessage(
      UUID.randomUUID().toString(),
      SubscriberState.streamName(subscriberId),
      SubscriberState.MessageType,
      SubscriberState(position).asJson,
      None,
      None,
    ).void

  def writePosition(subscriberId: String): Long => F[Unit] =
    p => writePosition(subscriberId, p)

  def writeGlobalPosition(subscriberId: String): MessageDb.Read.Message => F[Unit] =
    writePosition(subscriberId).compose(_.globalPosition + 1)

  // def subscribeToCategoryMessagesUnbounded(
  //   category: String,
  //   subscriberId: String,
  //   batchSize: Option[Long],
  //   correlation: Option[String],
  //   consumerGroupMember: Option[Long],
  //   consumerGroupSize: Option[Long],
  //   condition: Option[String],
  //   tickInterval: FiniteDuration,
  //   positionUpdateInterval: Long = 100L,
  // ): Stream[F, MessageDb.Read.Message] = 
  //   Stream.eval(loadPosition(subscriberId)).flatMap(start => 
  //     Stream.eval(Ref.of[F, Long](0)).flatMap(count => 
  //       getCategoryMessagesUnbounded(category, start, batchSize, correlation, consumerGroupMember, consumerGroupSize, condition, tickInterval)
  //         .evalTap(m => 
  //           count.update(_ + 1) *>
  //           count.get.flatMap(c => 
  //             if (c >= positionUpdateInterval)
  //               count.set(0L) *> writePosition(subscriberId, m.globalPosition)
  //           )
  //         )
  //     )
  //   )

  val writeMessages: Pipe[F, MessageDb.Write.Message, Long] = 
    _.evalMap(messageDb.writeMessage)

  val writeMessages_ : Pipe[F, MessageDb.Write.Message, Unit] = 
    _.through(writeMessages).void
}
