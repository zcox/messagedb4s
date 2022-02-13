package messagedb

import java.util.UUID
import java.time.LocalDateTime
import cats._
import cats.syntax.all._

/*
write_message enforces:
- id is uuid
- data is not null? need to verify
- data & metadata are jsonb

messages table enforces:
- id is uuid
- data & metadata are jsonb (but can be null?)
*/

// https://github.com/message-db/message-db/blob/master/database/types/message.sql
case class Message[A, B](
  id: UUID,
  streamName: String,
  `type`: String,
  position: Long,
  globalPosition: Long,
  data: A,
  metadata: B,
  time: LocalDateTime,
)

object Message {

  implicit object MessageBifunctor extends Bifunctor[Message] {
    override def bimap[A, B, C, D](fab: Message[A, B])(f: A => C, g: B => D): Message[C, D] =
      fab.copy(
        data = f(fab.data),
        metadata = g(fab.metadata),
      )
  }

  def toMessage2[A, B](m: Message[A, B]): Message2[A, B] = 
    m.copy(metadata = m.metadata.some)
}
