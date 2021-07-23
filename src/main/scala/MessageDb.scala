package messagedb

import io.circe.Json
import java.time.LocalDateTime
import fs2.Stream
import skunk._
import skunk.implicits._
import skunk.codec.all._
import skunk.circe.codec.all._
import cats.effect.Resource

trait MessageDb[F[_]] {

  // https://github.com/message-db/message-db/blob/master/database/functions/get-stream-messages.sql
  // http://docs.eventide-project.org/user-guide/message-db/server-functions.html#get-messages-from-a-stream
  def getStreamMessages(
      streamName: String,
      position: Option[Long],
      batchSize: Option[Long],
      condition: Option[String],
  ): Stream[F, MessageDb.Message]

  // https://github.com/message-db/message-db/blob/master/database/functions/write-message.sql
  // http://docs.eventide-project.org/user-guide/message-db/server-functions.html#write-a-message
  def writeMessage(
      //TODO should we try to declare id as a UUID here?
      id: String,
      streamName: String,
      `type`: String,
      data: Json,
      metadata: Option[Json],
      expectedVersion: Option[Long],
  ): F[Long]

}

object MessageDb {

  // https://github.com/message-db/message-db/blob/master/database/types/message.sql
  case class Message(
      id: String,
      streamName: String,
      `type`: String,
      position: Long,
      globalPosition: Long,
      //the message type declares data and metadata as varchar, so we have to use String here
      data: String,
      metadata: Option[String],
      time: LocalDateTime,
  )

  object Message {
    val codec = varchar ~ varchar ~ varchar ~ int8 ~ int8 ~ varchar ~ varchar.opt ~ timestamp
    val decoder = codec.gmap[Message]
  }

  object GetStreamMessages {
    type Arguments = String ~ Option[Long] ~ Option[Long] ~ Option[String]
    val query: Query[Arguments, Message] =
      sql"SELECT id, stream_name, type, position, global_position, data, metadata, time FROM get_stream_messages($varchar, ${int8.opt}, ${int8.opt}, ${varchar.opt})"
        .query(Message.decoder)
  }

  object WriteMessage {
    type Arguments = String ~ String ~ String ~ Json ~ Option[Json] ~ Option[Long]
    val query: Query[Arguments, Long] =
      sql"SELECT write_message($varchar, $varchar, $varchar, $jsonb, ${jsonb.opt}, ${int8.opt})"
        .query(int8)
  }

  def fromSession[F[_]](session: Session[F], chunkSize: Int = 32): Resource[F, MessageDb[F]] =
    for {
      getStreamMessagesQuery <- session.prepare(GetStreamMessages.query)
      writeMessageQuery <- session.prepare(WriteMessage.query)
    } yield new MessageDb[F] {
      override def getStreamMessages(
          streamName: String,
          position: Option[Long],
          batchSize: Option[Long],
          condition: Option[String],
      ): Stream[F, MessageDb.Message] =
        getStreamMessagesQuery.stream(streamName ~ position ~ batchSize ~ condition, chunkSize)

      override def writeMessage(
          id: String,
          streamName: String,
          `type`: String,
          data: Json,
          metadata: Option[Json],
          expectedVersion: Option[Long],
      ): F[Long] =
        writeMessageQuery.unique(id ~ streamName ~ `type` ~ data ~ metadata ~ expectedVersion)
    }

}
