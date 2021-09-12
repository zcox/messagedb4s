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
    val id1 = newUuid
    val streamName = s"test-$id1"
    val e1 = newUuid
    val j1 = toJson("""{"a":1,"b":2}""")
    val e2 = newUuid
    val j2 = toJson("""{"c":3,"d":4}""")
    messageDb.use { mdb =>
      for {
        p0 <- mdb.writeMessage(e1, streamName, "Test1", j1, None, None)
        p1 <- mdb.writeMessage(e2, streamName, "Test2", j2, None, None)
        ms1 <- mdb.getStreamMessages(streamName, None, None, None).compile.toList
      } yield {
        assertEquals(p0, 0L)
        assertEquals(p1, 1L)
        assertEquals(ms1.size, 2)
        assertMessage(e1, streamName, "Test1", 0L, j1, none, ms1(0))
        assertMessage(e2, streamName, "Test2", 1L, j2, none, ms1(1))
      }
    }
  }

}
