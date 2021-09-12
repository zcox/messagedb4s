package messagedb

import cats._
import cats.syntax.all._
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

  def apply[F[_]](
    messageDb: MessageDb[F],
    subscriberId: String,
    //TODO MonadError type
  )(implicit A: MonadError[F, Error]): Subscription[F] = 
  new Subscription[F] {

    val subscriberStreamName = s"subscriberPosition-$subscriberId"

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
  }
}
