package messagedb.write

import java.util.UUID
import cats._
import cats.syntax.all._
import io.circe._

/*
write_message enforces:
- id is uuid
- data is not null? need to verify
- data & metadata are jsonb

messages table enforces:
- id is uuid
- data & metadata are jsonb (but can be null?)
*/

case class Message[A, B](
  id: UUID,
  streamName: String,
  `type`: String,
  data: A,
  metadata: B,
  expectedVersion: Option[Long],
)

object Message {

  implicit object MessageBifunctor extends Bifunctor[Message] {
    override def bimap[A, B, C, D](fab: Message[A, B])(f: A => C, g: B => D): Message[C, D] =
      fab.copy(
        data = f(fab.data),
        metadata = g(fab.metadata),
      )
  }

  implicit class RichMessage[A, B](m: Message[A, B]) {

    def encode(implicit EA: Encoder[A], EB: Encoder[B]): Message[Json, Json] =
      m.bimap(
        Encoder[A].apply(_), 
        Encoder[B].apply(_),
      )

    def toMessage2: Message2[A, B] =
      m.copy(metadata = m.metadata.some)
  }

  implicit def toMessage2[A, B](m: Message[A, B]): Message2[A, B] =
    m.toMessage2

}
