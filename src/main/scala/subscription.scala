package messagedb

// import cats._
import cats.syntax.all._
import cats.effect._
import java.util.UUID
import io.circe._
import io.circe.syntax._
import io.circe.generic.semiauto._

trait Subscription[F[_]] {
  def writePosition(position: Long): F[Unit]
  def loadPosition(): F[Long]
}

object Subscription {

  case class SubscriberState(
    position: Long,
  )

  object SubscriberState {
    val MessageType = "Read"
    val Default = SubscriberState(0L)
    implicit val encoder: Encoder[SubscriberState] = deriveEncoder[SubscriberState]
    implicit val decoder: Decoder[SubscriberState] = deriveDecoder[SubscriberState]
  }


  def apply[F[_]: Concurrent](
    messageDb: MessageDb[F],
    subscriberId: String,
    // handler: MessageDb.Read.Message => F[Unit],
    // positionUpdateInterval: Int = 100,
    // messagesPerTick: Long = 100L,
    //TODO MonadError type
  )/*(implicit A: MonadError[F, Error])*/: Subscription[F] = 
  new Subscription[F] {

    val subscriberStreamName = s"subscriberPosition-$subscriberId"

    // var currentPosition: Long = -1
    // var messagesSinceLastPositionWrite: Int = 0

    override def writePosition(position: Long): F[Unit] = 
      messageDb.writeMessage(
        UUID.randomUUID().toString(),
        subscriberStreamName,
        SubscriberState.MessageType,
        SubscriberState(position).asJson,
        None,
        None,
      ).void

    override def loadPosition(): F[Long] = 
      for {
        o <- messageDb.getLastStreamMessage(subscriberStreamName)
        s <- o.fold(SubscriberState.Default.pure[F])(_.decodeData[SubscriberState].liftTo[F])
      } yield s.position

    // def updateReadPosition(position: Long): F[Unit] = {
    //   currentPosition = position
    //   messagesSinceLastPositionWrite += 1
    //   if (messagesSinceLastPositionWrite === positionUpdateInterval) { 
    //     messagesSinceLastPositionWrite = 0
    //     writePosition(position)
    //   } else Applicative[F].unit
    // }

    // def processBatch(messages: Stream[F, MessageDb.Read.Message]): F[Unit] = 
    //   messages.
    //     evalTap(handler)
    //     .evalTap(m => updateReadPosition(m.globalPosition))
    //     .compile.drain

    // def getNextBatchOfMessages(): Stream[F, MessageDb.Read.Message] = 
    //   messageDb.getStreamMessages(subscriberStreamName, (currentPosition + 1).some, messagesPerTick.some, none)

    // def tick(): F[Unit] = 
    //   processBatch(getNextBatchOfMessages())

    // def poll(): F[] = 




  }
}
