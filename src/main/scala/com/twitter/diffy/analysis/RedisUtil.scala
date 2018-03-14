package com.twitter.diffy.analysis
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import com.twitter.util.{Future, Promise}
import redis.RedisClient

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

trait RedisUtil {

  def serialise(value: Any): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close
    stream.toByteArray
  }

  def deserialise(bytes: Array[Byte]): Any = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val value = ois.readObject
    ois.close
    value
  }

  def getDataOrElse[A](redisClient: RedisClient, key: String, default: A)
                      (implicit ec: ExecutionContext): Future[A] = {
    val result = redisClient.get(key)
    val tp = new Promise[A]()
    result.onComplete({
      case Success(Some(byteString)) =>
        tp.setValue(deserialise(byteString.toArray).asInstanceOf[A])
      case Success(None) => tp.setValue(default)
      case Failure(e) => tp.setException(e)
    })
    tp
  }

  def writeData[A](redisClient: RedisClient, key: String, value: A)
                  (implicit ec: ExecutionContext): Future[Boolean] = {
    redisClient.del(key)
    val sf = redisClient.set(key, serialise(value))
    val tp = new Promise[Boolean]
    sf.onComplete {
      case Success(b) => tp.setValue(b)
      case Failure(e) => tp.setException(e)
    }
    tp
  }
}
