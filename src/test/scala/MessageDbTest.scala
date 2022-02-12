package messagedb

import munit._
import cats.effect._
import skunk._
import natchez.Trace.Implicits.noop
import java.util.UUID
import io.circe._
import io.circe.parser._
import cats.syntax.all._
import fs2.Stream
import scala.concurrent.duration._

//Assumes message-db is already running at localhost:5432, e.g. via `docker compose up -d`

class MessageDbTest extends CatsEffectSuite {

  val session: Resource[IO, Session[IO]] =
    Session.single(
      host = "localhost",
      port = 5432,
      user = "postgres",
      database = "message_store",
      password = Some("postgres"),
      parameters = Map(
        //messagedb's tables etc are in the message_store schema, not public schema
        "search_path" -> "message_store", 
        //http://docs.eventide-project.org/user-guide/message-db/server-functions.html#filtering-messages-with-a-sql-condition
        "message_store.sql_condition" -> "on"
      ) ++ Session.DefaultConnectionParameters,
    )

  val messageDb: Resource[IO, MessageDb[IO]] =
    session.flatMap(MessageDb.fromSession(_))

  def newUuid: UUID =
    UUID.randomUUID()

  def newCategory: String = 
    "test" + System.currentTimeMillis.toString

  def toJson(s: String): Json =
    parse(s).getOrElse(throw new NullPointerException())

  def assertMessage(
      id: UUID,
      streamName: String,
      `type`: String,
      position: Long,
      data: Json,
      metadata: Option[Json],
      m: MessageDb.Read.Message
  ) = {
    assertEquals(id, m.id)
    assertEquals(streamName, m.streamName)
    assertEquals(`type`, m.`type`)
    assertEquals(position, m.position)
    //globalPosition is affected by other messages in DB
    // assertEquals(globalPosition, m.globalPosition)
    assertEquals(data, m.data)
    assertEquals(metadata, m.metadata)
    //TODO time?
  }

  test("write and read messages") {
    val category = newCategory
    val id0 = newUuid
    val stream0 = s"$category-$id0"
    val id1 = newUuid
    val stream1 = s"$category-$id1"
    val e1 = newUuid
    val j1 = toJson("""{"a":1,"b":2}""")
    val e2 = newUuid
    val j2 = toJson("""{"c":3,"d":4}""")
    messageDb.use { mdb =>
      for {
        ms0 <- mdb.getStreamMessages(stream0, None, None, None).compile.toList
        c0 <- mdb.getCategoryMessages(category, None, None, None, None, None, None).compile.toList
        lm0 <- mdb.getLastStreamMessage(stream0)
        p0 <- mdb.writeMessage(e1, stream1, "Test1", j1, None, None)
        p1 <- mdb.writeMessage(e2, stream1, "Test2", j2, None, None)
        ms1 <- mdb.getStreamMessages(stream1, None, None, None).compile.toList
        c1 <- mdb.getCategoryMessages(category, None, None, None, None, None, None).compile.toList
        lm1 <- mdb.getLastStreamMessage(stream1)
        ms2 <- mdb.getStreamMessages(stream1, None, None, "(data->>'b')::int = 2".some).compile.toList
      } yield {
        assert(ms0.isEmpty)
        assert(c0.isEmpty)
        assert(lm0.isEmpty)

        assertEquals(p0, 0L)
        assertEquals(p1, 1L)
        
        assertEquals(ms1.size, 2)
        assertMessage(e1, stream1, "Test1", 0L, j1, none, ms1(0))
        assertMessage(e2, stream1, "Test2", 1L, j2, none, ms1(1))

        //TODO put more events in same category but different entity streams
        assertEquals(c1.size, 2)
        assertMessage(e1, stream1, "Test1", 0L, j1, none, c1(0))
        assertMessage(e2, stream1, "Test2", 1L, j2, none, c1(1))

        assert(lm1.isDefined)
        assertMessage(e2, stream1, "Test2", 1L, j2, none, lm1.get)

        assertEquals(ms2.size, 1)
        assertMessage(e1, stream1, "Test1", 0L, j1, none, ms2(0))
      }
    }
  }

  test("read stream unbounded") {
    val category = newCategory
    val id = newUuid
    val stream = s"$category-$id"
    val m0 = MessageDb.Write.Message(newUuid, stream, "test", toJson("""{"a":1}"""), none, none)
    val m1 = MessageDb.Write.Message(newUuid, stream, "test", toJson("""{"a":2}"""), none, none)
    val m2 = MessageDb.Write.Message(newUuid, stream, "test", toJson("""{"a":3}"""), none, none)
    val m3 = MessageDb.Write.Message(newUuid, stream, "test", toJson("""{"a":4}"""), none, none)
    messageDb.use { mdb => 
      val input = Stream(m0, m1) ++ Stream.sleep_[IO](1.second) ++ Stream(m2, m3)
      val writes = input.through(mdb.writeMessages)
      val reads = mdb.getCategoryMessagesUnbounded(category, 0, 1L.some, none, none, none, none, 100.millis)/*.evalTap(m => IO(println(m)))*/.take(4)
      for {
        ms <- reads.concurrently(writes).compile.toList
      } yield {
        assertEquals(ms.size, 4)
        assertMessage(m0.id, m0.streamName, m0.`type`, 0, m0.data, m0.metadata, ms(0))
        assertMessage(m1.id, m1.streamName, m1.`type`, 1, m1.data, m1.metadata, ms(1))
        assertMessage(m2.id, m2.streamName, m2.`type`, 2, m2.data, m2.metadata, ms(2))
        assertMessage(m3.id, m3.streamName, m3.`type`, 3, m3.data, m3.metadata, ms(3))
      }
    }
  }

}
