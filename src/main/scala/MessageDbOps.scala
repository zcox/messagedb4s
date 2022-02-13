package messagedb

import cats.syntax.all._
import cats.effect._
import fs2.{Stream, Pipe}
import scala.concurrent.duration._
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
      if (gettingSamePosition || lastPositionGot == Long.MinValue)
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
          Stream.eval(state.update(_.copy(lastPositionGot = s.nextPositionToGet))).drain ++
          getMessages(s.nextPositionToGet)
        )
      ).flatten.evalTap(m => 
        state.update(_.copy(nextPositionToGet = getPosition(m) + 1))
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
      s <- o.fold(SubscriberState.Default.pure[F])(_.dataAs[SubscriberState].liftTo[F])
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

  def subscribe(
    category: String,
    subscriberId: String,
    f: MessageDb.Read.Message => F[Unit],
    batchSize: Option[Long] = 100L.some,
    correlation: Option[String] = none,
    consumerGroupMember: Option[Long] = none,
    consumerGroupSize: Option[Long] = none,
    condition: Option[String] = none,
    tickInterval: FiniteDuration = 1.second,
  ): Stream[F, Unit] = 
    getCategoryMessagesUnbounded(category, subscriberId, batchSize, correlation, consumerGroupMember, consumerGroupSize, condition, tickInterval)
      .evalTap(f)
      .evalTap(writeGlobalPosition(subscriberId))
      .void

  //TODO subscriber util that writes position every n messages

  def writePosition(subscriberId: String, position: Long): F[Unit] = 
    messageDb.writeMessage(
      UUID.randomUUID(),
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

  def getAllStreamMessages(
    streamName: String,
    position: Option[Long] = 0L.some,
    batchSize: Option[Long] = -1L.some,
    condition: Option[String] = none,
  ): Stream[F, MessageDb.Read.Message] = 
    messageDb.getStreamMessages(streamName, position, batchSize, condition)

  def getFirstStreamMessage(
    streamName: String,
    position: Option[Long] = 0L.some,
    condition: Option[String] = none,
  ): F[Option[MessageDb.Read.Message]] = 
    getAllStreamMessages(
      streamName = streamName,
      position = position,
      batchSize = 1L.some,
      condition = condition,
    ).compile.last

  def isStreamEmpty(streamName: String): F[Boolean] = 
    getFirstStreamMessage(streamName).map(_.isEmpty)

  def writeMessage_(
      id: UUID,
      streamName: String,
      `type`: String,
      data: Json,
      metadata: Option[Json],
      expectedVersion: Option[Long],
  ): F[Unit] = 
    messageDb.writeMessage(id, streamName, `type`, data, metadata, expectedVersion).void

  def writeMessage_(message: MessageDb.Write.Message): F[Unit] =
    messageDb.writeMessage(message).void

  val writeMessages: Pipe[F, MessageDb.Write.Message, Long] = 
    _.evalMap(messageDb.writeMessage)

  val writeMessages_ : Pipe[F, MessageDb.Write.Message, Unit] = 
    _.through(writeMessages).void
}
