package messagedb

import cats._
import cats.syntax.all._
import io.circe._

package object read {

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

  implicit class RichJsonMessage2(m: JsonMessage2) {

    def decode[A: Decoder, B: Decoder]: Message2[Decoder.Result[A], Decoder.Result[B]] =
      m.bimap(
        Decoder[A].decodeJson(_), 
        Decoder[B].decodeJson(_),
      )

    def as[A: Decoder, B: Decoder]: Decoder.Result[Message2[A, B]] = {
      val d = m.decode[A, B]
      for {
        a <- d.data
        b <- d.metadata.fold(none[B].asRight[DecodingFailure])(_.map(_.some))
      } yield d.copy(data = a, metadata = b)
    }

    def dataAs[A: Decoder]: Decoder.Result[A] = 
      Decoder[A].decodeJson(m.data)

    def metadataAs[A: Decoder]: Decoder.Result[A] = 
      m.metadata
        .toRight[DecodingFailure](DecodingFailure("metadata field does not exist", List.empty))
        .flatMap(Decoder[A].decodeJson(_))

    def metadataAsO[A: Decoder]: Option[Decoder.Result[A]] =
      m.metadata.map(Decoder[A].decodeJson(_))
  }

}
