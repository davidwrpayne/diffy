package com.twitter.diffy.analysis

import akka.actor.ActorSystem
import com.twitter.diffy.analysis.InMemoryDifferenceCollector.DifferenceResultNotFoundException
import com.twitter.diffy.thriftscala.DifferenceResult
import com.twitter.logging.Logger
import com.twitter.util.Future
import redis.RedisClient

import scala.collection.mutable
import scala.concurrent.ExecutionContext


class RedisStoreDifferenceCollector(redisClient: RedisClient)
                                   (implicit ec: ExecutionContext)
  extends DifferenceCollector with RedisUtil {

  val basePathName = "Diffy"
  val log = Logger(classOf[RedisStoreDifferenceCollector])

  val fieldsObjectKey = "Diffy:FieldsObject"

  val emptyFields = mutable.Map.empty[Field, mutable.Queue[DifferenceResult]]
  var fields = mutable.Map.empty[Field, mutable.Queue[DifferenceResult]]
  val requestsPerField = 5

  private[this] def sanitizePath(p: String) = p.stripSuffix("/").stripPrefix("/")

  override def create(dr: DifferenceResult): Unit = {
    for {
      fs <- getDataOrElse(redisClient,fieldsObjectKey,emptyFields)
    } yield {
      dr.differences foreach { case (path, _) =>
        val queue =
          fs.getOrElseUpdate(
            Field(dr.endpoint, sanitizePath(path)),
            mutable.Queue.empty[DifferenceResult]
          )

        if (queue.size < requestsPerField) {
          queue.enqueue(dr)
        }
      }
      writeData( redisClient,fieldsObjectKey, fs)
    }
  }

  override def prefix(field: Field): Future[Iterable[DifferenceResult]] = {
    for {
      fields <- getDataOrElse(redisClient,fieldsObjectKey,emptyFields)
      res = (fields flatMap {
        case (Field(endpoint, path), value)
          if endpoint == field.endpoint && path.startsWith(field.prefix) => value
        case _ => Nil
      }).toSeq.distinct
    } yield {
      res
    }
  }

  override def apply(id: Long): Future[DifferenceResult] = {
    for {
      fields <- getDataOrElse(redisClient,fieldsObjectKey,emptyFields)
      // Collect first instance of this difference showing up in all the fields
      res <- fields.toStream map { case (field, queue) =>
        queue.find {
          _.id == id
        }
      } collectFirst {
        case Some(dr) => dr
      } match {
        case Some(dr) => Future.value(dr)
        case None => DifferenceResultNotFoundException
      }
    }
      yield {
        res
      }
  }

  override def clear(): Future[Unit] = {
    log.info("clear called ")
    clearFields(redisClient).map(_ => ())
  }

  def clearFields(client: RedisClient): Future[Boolean] = {
    writeData(redisClient, fieldsObjectKey, emptyFields)
  }

}
