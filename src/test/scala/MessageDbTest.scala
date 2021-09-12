package messagedb

import munit._
import cats.effect._
import skunk._
import natchez.Trace.Implicits.noop
import java.util.UUID
import io.circe._
import io.circe.parser._
import cats.syntax.all._

//Assumes message-db is already running at localhost:5432, e.g. via `docker compose up -d`

class MessageDbTest extends CatsEffectSuite {

  val session: Resource[IO, Session[IO]] =
    Session.single(
      host = "localhost",
      port = 5432,
      user = "message_store",
      database = "message_store",
      password = Some("message_store")
    )

  val messageDb: Resource[IO, MessageDb[IO]] =
    session.flatMap(MessageDb.fromSession(_))

  def newUuid: String =
    UUID.randomUUID.toString

  def newCategory: String = 
    "test" + System.currentTimeMillis.toString

  def toJson(s: String): Json =
    parse(s).getOrElse(throw new NullPointerException())

  def assertMessage(
      id: String,
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
    assertEquals(data, toJson(m.data))
    assertEquals(metadata, m.metadata.map(toJson))
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
      }
    }
  }

}
