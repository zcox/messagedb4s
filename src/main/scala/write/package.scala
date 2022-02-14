package messagedb

import cats.effect.Temporal
import cats._
import cats.syntax.all._
import io.circe._

package object write {

  type JsonMessage = Message[Json, Json]
  type Message2[A, B] = Message[A, Option[B]]
  type JsonMessage2 = Message2[Json, Json]

  implicit object Message2Bifunctor extends Bifunctor[Message2] {
    override def bimap[A, B, C, D](fab: Message2[A, B])(f: A => C, g: B => D): Message2[C, D] =
      fab.copy(
        data = f(fab.data),
        metadata = fab.metadata.map(g),
      )
  }

  implicit class RichMessage2[A, B](m: Message2[A, B]) {

    def encode(implicit EA: Encoder[A], EB: Encoder[B]): Message2[Json, Json] =
      m.bimap(
        Encoder[A].apply(_), 
        Encoder[B].apply(_),
      )

    def toMessageUnsafe: Message[A, B] =
      m.copy(metadata = m.metadata.get)

    def toMessage(default: => B): Message[A, B] =
      m.copy(metadata = m.metadata.getOrElse(default))
  }

  implicit def messageDbOps[F[_]: Temporal](messageDb: MessageDb[F]): MessageDbOps[F] = 
    MessageDbOps[F](messageDb)
}
