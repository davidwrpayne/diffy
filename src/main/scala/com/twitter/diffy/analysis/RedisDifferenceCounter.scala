package com.twitter.diffy.analysis

import akka.dispatch.Dispatcher
import com.twitter.diffy.compare.Difference
import com.twitter.util.{Future, Promise}
import redis.RedisClient

import scala.collection.{immutable, mutable}
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class RedisDifferenceCounter(redisClient: RedisClient,id: String)
                            (implicit executionContext: ExecutionContext)
  extends DifferenceCounter with RedisUtil {

  val EmptyData = mutable.HashMap.empty[String, EndpointMetadataObject]

  var data: mutable.HashMap[String, EndpointMetadataObject] = EmptyData


  /**
    * Looks up endpoint in current collection.
    * if the endpoint hasn't been recorded it creates new endpointMetadata and stores it in hashmap at
    * endpoint -> endpoint Metadata
    * It then calls add on that endpointmetadata and passes in diffs
    * add() increments total of endpointmetatdata by one
    * if diffs size > 0 increments diffcounter
    * for each diff looks up metadata by fieldpath of diff in fields hashmap
    * then passes diffs to InMemoryFieldMetaData
    * which increments the count of that fieldmetadata
    * and increments sibling count by the size of diffs and returns that value
    * @param endpoint
    * @param diffs
    * @return
    */
  override def count(endpoint: String, diffs: Map[String, Difference]): Future[Unit] = {

    for {
      // add the endoint to the endpointList and write to redis
      endpointSet <- getDataOrElse(redisClient, Keys.endpointSet, Set.empty[String])
      newSet = endpointSet ++ Set(endpoint)
      _ <- writeData(redisClient,Keys.endpointSet, newSet)

      // write the endpoint count and diffrenceCount to redis
      total <- getDataOrElse(redisClient,Keys.endpointTotalCount(endpoint),0)
      newTotal = total + 1
      _ <- writeData(redisClient,Keys.endpointTotalCount(endpoint),newTotal)

      differenceCount <- getDataOrElse(redisClient,Keys.endpointDifferencesCount(endpoint),0)
      newdifferenceCount = differenceCount + 1
      _ <- writeData(redisClient, Keys.endpointDifferencesCount(endpoint), newdifferenceCount)

      // add all the fields for the diff to the list and write to redis
      newFields = diffs.keys
      allFields <- getDataOrElse(redisClient, Keys.endpointFieldSet(endpoint), Set.empty[String])
      modifiedFields = allFields ++ newFields.toSet
      _ <- writeData(redisClient, Keys.endpointFieldSet(endpoint), modifiedFields)

      // all that is left is to update counts for fields here in newFields set.

    } yield {}

    val results: immutable.Iterable[Future[Unit]] = diffs.map({ k =>
      val fieldKey = k._1
        for {
          fieldCount <- getDataOrElse(redisClient,Keys.endpointFieldCount(endpoint,fieldKey), 0)
          siblingCount <- getDataOrElse(redisClient,Keys.endpointFieldSiblingCount(endpoint, fieldKey),0)
          newFieldCount = fieldCount + 1
          newSiblingCount = siblingCount + diffs.size
          _ <- writeData(redisClient,Keys.endpointFieldCount(endpoint,fieldKey),newFieldCount)
          _ <- writeData(redisClient,Keys.endpointFieldSiblingCount(endpoint,fieldKey), newSiblingCount)
        }
          yield {()}
    })
    Future.collect(results.toSeq).map(_ => () )
  }


  /**
    * gets all endpoint metadata filtered by totals > 0
    * @return
    */
  override def endpoints: Future[Map[String, EndpointMetadataObject]] = {
    for {
      endpointSet <- getDataOrElse(redisClient, Keys.endpointSet, Set.empty[String])
      val fSeqMetadata: Set[Future[(String, EndpointMetadataObject)]] = endpointSet.map { endpoint =>
        for {
          count <- getDataOrElse(redisClient, Keys.endpointDifferencesCount(endpoint), 0)
          total <- getDataOrElse(redisClient, Keys.endpointTotalCount(endpoint), 0)
        } yield {
          (endpoint, EndpointMetadataObject(count, total))
        }
      }
      result <- Future.collect(fSeqMetadata.toSeq)

    } yield {
      result.toMap
    }
  }

  /**
    * returns the fields map
    * @param endpoint
    * @return
    */
  override def fields(endpoint: String): Future[Map[String, FieldMetadata]] = {
    for {
      fieldSet <- getDataOrElse(redisClient, Keys.endpointFieldSet(endpoint), Set.empty[String])
        val fSeqMetadata = fieldSet.map { field =>
          for {
            fieldCount <- getDataOrElse(redisClient, Keys.endpointFieldCount(endpoint,field), 0)
            fieldSiblingCount <- getDataOrElse(redisClient, Keys.endpointFieldSiblingCount(endpoint,field), 0)
          } yield {
            (field, FieldMetaDataObject(fieldCount, fieldSiblingCount))
          }
        }
      result <- Future.collect(fSeqMetadata.toSeq)

    } yield {
      result.toMap
    }
  }

  /**
    * clears all data
    * @return
    */
  override def clear(): Future[Unit] = {

    for {
      keys <- getAllKeysForSelf(redisClient)
      res = keys.map({ key =>
        deleteKey(redisClient, key)
      })
      _ <- Future.collect(res)
    } yield {
      ()
    }

  }

  def deleteKey(redisClient: RedisClient, key: String): Future[Unit] = {
    val tp = new Promise[Unit]()
    val result = redisClient.del(key)
    result.onComplete({
      case Success(_) => tp.setValue(Unit)
      case Failure(e) => tp.setException(e)
    })
    tp
  }

  def getAllKeysForSelf(redisClient: RedisClient): Future[Vector[String]] = {
    val tp = new Promise[Vector[String]]()
    val result = redisClient.keys(s"${Keys.RedisKey}:*")
    result.onComplete({
      case Success(a) => tp.setValue(a.asInstanceOf[Vector[String]])
      case Failure(e) => tp.setException(e)
    })
    tp
  }


  object Keys {
    val RedisKey = s"Diffy:RedisDifferenceCounter:$id"
    val endpointSet = s"$RedisKey:endpointList"
    val endpointPrefix = s"$RedisKey:endpoints"
    def endpointTotalCount(endpoint: String): String = s"$endpointPrefix:$endpoint:TotalCount"
    def endpointDifferencesCount(endpoint: String): String = s"$endpointPrefix:$endpoint:DifferencesCount"
    def endpointFieldCount(endpoint: String, field: String): String = s"$endpointPrefix:$endpoint:$field:Count"
    def endpointFieldSiblingCount(endpoint: String, field: String): String =
      s"$endpointPrefix:$endpoint:$field:SiblingCount"
    def endpointFieldSet(endpoint: String): String = s"$endpointPrefix:$endpoint:fieldList"
  }

}

case class EndpointMetadataObject(difference: Int, t: Int) extends EndpointMetadata {
  override def differences: Int = difference
  override def total: Int = t
}

case class FieldMetaDataObject(d: Int, w: Int) extends FieldMetadata {
  override def differences: Int = d
  override def weight: Int = w
}
