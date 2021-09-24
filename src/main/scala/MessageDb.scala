package messagedb

import cats._
import io.circe.{Json, Decoder, Error}
import io.circe.parser.decode
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
  ): Stream[F, MessageDb.Read.Message]

  // https://github.com/message-db/message-db/blob/master/database/functions/get-category-messages.sql
  def getCategoryMessages(
    category: String,
    position: Option[Long],
    batchSize: Option[Long],
    correlation: Option[String],
    consumerGroupMember: Option[Long],
    consumerGroupSize: Option[Long],
    condition: Option[String],
  ): Stream[F, MessageDb.Read.Message]

  // https://github.com/message-db/message-db/blob/master/database/functions/get-last-stream-message.sql
  def getLastStreamMessage(
    streamName: String,
  ): F[Option[MessageDb.Read.Message]]

  // https://github.com/message-db/message-db/blob/master/database/functions/write-message.sql
  // http://docs.eventide-project.org/user-guide/message-db/server-functions.html#write-a-message
  def writeMessage(
      id: String,
      streamName: String,
      `type`: String,
      data: Json,
      metadata: Option[Json],
      expectedVersion: Option[Long],
  ): F[Long]

  def writeMessage(message: MessageDb.Write.Message): F[Long] =
    writeMessage(
      message.id,
      message.streamName,
      message.`type`,
      message.data,
      message.metadata,
      message.expectedVersion,
    )

}

object MessageDb {

  def mapK[F[_], G[_]](m: MessageDb[F])(f: F ~> G): MessageDb[G] = 
    new MessageDb[G] {
      override def getStreamMessages(
          streamName: String,
          position: Option[Long],
          batchSize: Option[Long],
          condition: Option[String],
      ): Stream[G, MessageDb.Read.Message] = 
        m.getStreamMessages(streamName, position, batchSize, condition).translate(f)

      override def getCategoryMessages(
        category: String,
        position: Option[Long],
        batchSize: Option[Long],
        correlation: Option[String],
        consumerGroupMember: Option[Long],
        consumerGroupSize: Option[Long],
        condition: Option[String],
      ): Stream[G, MessageDb.Read.Message] = 
        m.getCategoryMessages(category, position, batchSize, correlation, consumerGroupMember, consumerGroupSize, condition).translate(f)

      override def getLastStreamMessage(
        streamName: String,
      ): G[Option[MessageDb.Read.Message]] = 
        f(m.getLastStreamMessage(streamName))

      override def writeMessage(
          id: String,
          streamName: String,
          `type`: String,
          data: Json,
          metadata: Option[Json],
          expectedVersion: Option[Long],
      ): G[Long] = 
        f(m.writeMessage(id, streamName, `type`, data, metadata, expectedVersion))
    }

  //TODO Sync?
  def useEachTime[F[_]: cats.effect.Sync](r: Resource[F, MessageDb[F]]): MessageDb[F] = 
    new MessageDb[F] {
      override def getStreamMessages(
          streamName: String,
          position: Option[Long],
          batchSize: Option[Long],
          condition: Option[String],
      ): Stream[F, MessageDb.Read.Message] = 
        Stream.resource(r).flatMap(_.getStreamMessages(streamName, position, batchSize, condition))

      override def getCategoryMessages(
        category: String,
        position: Option[Long],
        batchSize: Option[Long],
        correlation: Option[String],
        consumerGroupMember: Option[Long],
        consumerGroupSize: Option[Long],
        condition: Option[String],
      ): Stream[F, MessageDb.Read.Message] = 
        Stream.resource(r).flatMap(_.getCategoryMessages(category, position, batchSize, correlation, consumerGroupMember, consumerGroupSize, condition))

      override def getLastStreamMessage(
        streamName: String,
      ): F[Option[MessageDb.Read.Message]] = 
        r.use(_.getLastStreamMessage(streamName))

      override def writeMessage(
          id: String,
          streamName: String,
          `type`: String,
          data: Json,
          metadata: Option[Json],
          expectedVersion: Option[Long],
      ): F[Long] = 
        r.use(_.writeMessage(id, streamName, `type`, data, metadata, expectedVersion))
    }

  object Write {
    case class Message(
        id: String,
        streamName: String,
        `type`: String,
        data: Json,
        metadata: Option[Json],
        expectedVersion: Option[Long],
    )
  }

  object Read {
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
    ) {
      def decodeData[A: Decoder]: Either[Error, A] = 
        decode[A](data)
      def decodeMetadata[A: Decoder]: Either[Error, A] = 
        metadata
          .toRight[Error](ParsingFailure("metadata field does not exist", new RuntimeException("metadata field does not exist")))
          .flatMap(decode[A])
    }

    object Message {
      val codec = varchar ~ varchar ~ varchar ~ int8 ~ int8 ~ varchar ~ varchar.opt ~ timestamp
      val decoder = codec.gmap[Message]
    }
  }

  object GetStreamMessages {
    type Arguments = String ~ Option[Long] ~ Option[Long] ~ Option[String]
    val query: Query[Arguments, Read.Message] =
      sql"SELECT id, stream_name, type, position, global_position, data, metadata, time FROM get_stream_messages($varchar, ${int8.opt}, ${int8.opt}, ${varchar.opt})"
        .query(Read.Message.decoder)
  }

  object GetCategoryMessages {
    type Arguments = String ~ Option[Long] ~ Option[Long] ~ Option[String] ~ Option[Long] ~ Option[Long] ~ Option[String]
    val query: Query[Arguments, Read.Message] =
      sql"SELECT id, stream_name, type, position, global_position, data, metadata, time FROM get_category_messages($varchar, ${int8.opt}, ${int8.opt}, ${varchar.opt}, ${int8.opt}, ${int8.opt}, ${varchar.opt})"
        .query(Read.Message.decoder)
  }

  object GetLastStremMessage {
    type Arguments = String
    val query: Query[Arguments, Read.Message] = 
      sql"SELECT id, stream_name, type, position, global_position, data, metadata, time FROM get_last_stream_message($varchar)"
        .query(Read.Message.decoder)
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
      getCategoryMessageQuery <- session.prepare(GetCategoryMessages.query)
      getLastStreamMessageQuery <- session.prepare(GetLastStremMessage.query)
      writeMessageQuery <- session.prepare(WriteMessage.query)
    } yield new MessageDb[F] {

      override def getStreamMessages(
          streamName: String,
          position: Option[Long],
          batchSize: Option[Long],
          condition: Option[String],
      ): Stream[F, MessageDb.Read.Message] =
        getStreamMessagesQuery.stream(streamName ~ position ~ batchSize ~ condition, chunkSize)

      override def getCategoryMessages(
        category: String,
        position: Option[Long],
        batchSize: Option[Long],
        correlation: Option[String],
        consumerGroupMember: Option[Long],
        consumerGroupSize: Option[Long],
        condition: Option[String],
      ): Stream[F, MessageDb.Read.Message] =
        getCategoryMessageQuery.stream(category ~ position ~ batchSize ~ correlation ~ consumerGroupMember ~ consumerGroupSize ~ condition, chunkSize)

      override def getLastStreamMessage(
        streamName: String,
      ): F[Option[MessageDb.Read.Message]] = 
        getLastStreamMessageQuery.option(streamName)

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
