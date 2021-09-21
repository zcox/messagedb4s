package messagedb

import cats.syntax.all._
import cats.effect._
import fs2.{Stream, Pipe}
import scala.concurrent.duration.FiniteDuration

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

}

case class MessageDbOps[F[_]: Temporal](messageDb: MessageDb[F]) {
  import MessageDbOps._

  def getStreamMessagesUnbounded(
    streamName: String,
    position: Long,
    batchSize: Option[Long],
    condition: Option[String],
    tickInterval: FiniteDuration,
  ): Stream[F, MessageDb.Read.Message] = 
    Stream.eval(Ref.of[F, PollingState](PollingState(position))).flatMap(state =>
      Stream.repeatEval(
        state.get.map(s => 
          s.sleepIfGettingSamePosition[F](tickInterval) ++
          messageDb.getStreamMessages(streamName, s.nextPositionToGet.some, batchSize, condition)
        )
      ).flatten.evalTap(m => 
        state.update(_.update(m.position + 1))
      )
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
    Stream.eval(Ref.of[F, PollingState](PollingState(position))).flatMap(state =>
      Stream.repeatEval(
        state.get.map(s => 
          s.sleepIfGettingSamePosition[F](tickInterval) ++
          messageDb.getCategoryMessages(category, s.nextPositionToGet.some, batchSize, correlation, consumerGroupMember, consumerGroupSize, condition)
        )
      ).flatten.evalTap(m => 
        state.update(_.update(m.globalPosition + 1))
      )
    )

  val writeMessages: Pipe[F, MessageDb.Write.Message, Long] = 
    _.evalMap(messageDb.writeMessage)

  val writeMessages_ : Pipe[F, MessageDb.Write.Message, Unit] = 
    _.through(writeMessages).void
}
