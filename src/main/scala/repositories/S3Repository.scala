package repositories 

import java.net.URI

import model.S3Document

import zio.*
import zio.s3._
import zio.stream.{ZSink, ZStream}

import zio.dynamodb.examples.dynamodblocal.DynamoDB
import zio.dynamodb.DynamoDBQuery.{ createTable, put }
import zio.dynamodb._

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.S3Exception

trait S3Repository:

  def createBucket(bucket: String): ZIO[Any, Throwable, Unit]

  def allDocuments(bucket: String): ZStream[Any, Throwable, S3Document]

  def copy(bucketIn: String, keyIn: String, bucketOut: String, keyOut: String): ZIO[Any, Throwable, Unit]

  def sample(bucket: String, count: Int): ZIO[Any, Throwable, Unit]


object S3Repository:

  def layer: ZLayer[S3 & DynamoRepository, Nothing, S3Repository] = ZLayer {
    for { 
      s3 <- ZIO.service[S3]
      repo <- ZIO.service[DynamoRepository]
    } yield S3RepositoryLive(s3)
  }

case class S3RepositoryLive(s3: S3) extends S3Repository:

  implicit class toDocument(summary: S3ObjectSummary): 
    def asDocument = S3Document(summary.key, None, List(), None)
  
  def allDocuments(bucket: String) = s3.listAllObjects(bucket).tap(summary => Console.printLine(summary.toString)).map(_.asDocument)
  
  val content = Chunk.fromArray("a sample file content".getBytes("UTF-8"))
  def writeSample(bucket: String, i: Int) = s3.putObject(bucket, s"sample/$i.txt", content.size, ZStream.fromChunk(content))
  def sample(bucket: String, count: Int) = 
    ZIO.foreach(1 to count)(i => writeSample(bucket, i)).unit

  def copy(bucketIn: String, keyIn: String, bucketOut: String, keyOut: String): ZIO[Any, Throwable, Unit] = for {
    metadata <- s3.getObjectMetadata(bucketIn, keyIn)
    stream = s3.getObject(bucketIn, keyIn)
    _ <- s3.putObject(bucketOut, keyOut, metadata.contentLength, stream) 
  } yield ()

  def createBucket(bucket: String) = for {
    exists <- s3.isBucketExists(bucket)
    _ <- if (exists) ZIO.unit else s3.createBucket(bucket)
  } yield ()