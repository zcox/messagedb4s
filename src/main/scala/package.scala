import cats.effect.Temporal

package object messagedb {

  implicit def messageDbOps[F[_]: Temporal](messageDb: MessageDb[F]): MessageDbOps[F] = 
    MessageDbOps[F](messageDb)
}
