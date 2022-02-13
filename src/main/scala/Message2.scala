package messagedb

//TODO RichMessage2?

object Message2 {

  def getMetadata[A, B](m: Message2[A, B]): Message[A, B] =
    m.copy(metadata = m.metadata.get)

  def getMetadataOrElse[A, B](m: Message2[A, B], default: => B): Message[A, B] =
    m.copy(metadata = m.metadata.getOrElse(default))

}
