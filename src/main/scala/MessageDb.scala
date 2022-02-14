package messagedb

import cats._
import io.circe.Json
import java.util.UUID
import fs2.Stream
import skunk._
import skunk.implicits._
import skunk.codec.all._
import skunk.circe.codec.all._
import cats.effect.Resource

/*

Here is an example of the error from writeMessage when expectedVersion does not match:

[ERROR] Exception in thread "main" skunk.exception.PostgresErrorException:
[ERROR] 🔥
[ERROR] 🔥  Postgres ERROR P0001 raised in exec_stmt_raise (pl_exec.c:3337)
[ERROR] 🔥
[ERROR] 🔥    Problem: Wrong expected version: 0 (Stream:
[ERROR] 🔥             identity-2ee1df30-f3f6-4566-af9f-eb0828834fb8, Stream Version: -1).
[ERROR] 🔥
[ERROR] 🔥  The statement under consideration was defined
[ERROR] 🔥    at /Users/zcox/code/zcox/messagedb4s/src/main/scala/MessageDb.scala:203
[ERROR] 🔥
[ERROR] 🔥    SELECT write_message($1, $2, $3, $4, $5, $6)
[ERROR] 🔥
[ERROR] 🔥  and the arguments were provided
[ERROR] 🔥    at /Users/zcox/code/zcox/messagedb4s/src/main/scala/MessageDb.scala:247
[ERROR] 🔥
[ERROR] 🔥    $1 varchar    fc9b466d-998c-448f-8a17-3888aba5e573
[ERROR] 🔥    $2 varchar    identity-2ee1df30-f3f6-4566-af9f-eb0828834fb8
[ERROR] 🔥    $3 varchar    Registered
[ERROR] 🔥    $4 jsonb      {"userId":"2ee1df30-f3f6-4566-af9f-eb0828834fb8","email":"user@site.com","passwordHash":"$2a$10$9CdhK9eSUwm6EHtV2kxwYu6mNEUziorQW7K1c/CTDqc9OPlYxDJ/u"}
[ERROR] 🔥    $5 jsonb      {"userId":"ae485b99-b820-48d3-a8d5-9a1a606b6ed6","traceId":"2ee1df30-f3f6-4566-af9f-eb0828834fb8"}
[ERROR] 🔥    $6 int8       0
[ERROR] 🔥
[ERROR] 🔥  If this is an error you wish to trap and handle in your application, you can do
[ERROR] 🔥  so with a SqlState extractor. For example:
[ERROR] 🔥
[ERROR] 🔥    doSomething.recoverWith { case SqlState.RaiseException(ex) =>  ...}
[ERROR] 🔥
[ERROR]
[ERROR] skunk.exception.PostgresErrorException: Wrong expected version: 0 (Stream: identity-2ee1df30-f3f6-4566-af9f-eb0828834fb8, Stream Version: -1).

May want to consider translating that into something more explicit, that could be handled by clients.

*/

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
      id: UUID,
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
          id: UUID,
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
          id: UUID,
          streamName: String,
          `type`: String,
          data: Json,
          metadata: Option[Json],
          expectedVersion: Option[Long],
      ): F[Long] = 
        r.use(_.writeMessage(id, streamName, `type`, data, metadata, expectedVersion))
    }

  object Write {
    type Message = write.JsonMessage2

    object Message {
      def apply(
        id: UUID,
        streamName: String,
        `type`: String,
        data: Json,
        metadata: Option[Json],
        expectedVersion: Option[Long],
      ): Message = 
        write.Message(id, streamName, `type`, data, metadata, expectedVersion)
    }
  }

  object Read {

    // https://github.com/message-db/message-db/blob/master/database/types/message.sql
    type Message = read.JsonMessage2

    object Message {
      val codec = uuid ~ varchar ~ varchar ~ int8 ~ int8 ~ jsonb ~ jsonb.opt ~ timestamp
      val decoder = codec.gmap[Message]
    }
  }

  object GetStreamMessages {
    type Arguments = String ~ Option[Long] ~ Option[Long] ~ Option[String]
    val query: Query[Arguments, Read.Message] =
      sql"SELECT uuid(id), stream_name, type, position, global_position, jsonb(data), jsonb(metadata), time FROM get_stream_messages($varchar, ${int8.opt}, ${int8.opt}, ${varchar.opt})"
        .query(Read.Message.decoder)
  }

  object GetCategoryMessages {
    type Arguments = String ~ Option[Long] ~ Option[Long] ~ Option[String] ~ Option[Long] ~ Option[Long] ~ Option[String]
    val query: Query[Arguments, Read.Message] =
      sql"SELECT uuid(id), stream_name, type, position, global_position, jsonb(data), jsonb(metadata), time FROM get_category_messages($varchar, ${int8.opt}, ${int8.opt}, ${varchar.opt}, ${int8.opt}, ${int8.opt}, ${varchar.opt})"
        .query(Read.Message.decoder)
  }

  object GetLastStremMessage {
    type Arguments = String
    val query: Query[Arguments, Read.Message] = 
      sql"SELECT uuid(id), stream_name, type, position, global_position, jsonb(data), jsonb(metadata), time FROM get_last_stream_message($varchar)"
        .query(Read.Message.decoder)
  }

  object WriteMessage {
    type Arguments = UUID ~ String ~ String ~ Json ~ Option[Json] ~ Option[Long]
    val query: Query[Arguments, Long] =
      sql"SELECT write_message($uuid::varchar, $varchar, $varchar, $jsonb, ${jsonb.opt}, ${int8.opt})"
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
          id: UUID,
          streamName: String,
          `type`: String,
          data: Json,
          metadata: Option[Json],
          expectedVersion: Option[Long],
      ): F[Long] =
        writeMessageQuery.unique(id ~ streamName ~ `type` ~ data ~ metadata ~ expectedVersion)
    }

}
