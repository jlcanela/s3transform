package services

import zio.*
import repositories.DynamoRepository
import repositories.S3Repository
import model.S3Document

trait S3Transfer:

    def setup: ZIO[Any, Throwable, Unit]

    def transfer(bucket: String, outputBucket: String): ZIO[Any, Throwable, Unit]

    def scan(bucket: String): ZIO[Any, Throwable, Unit]
    def copy(incremental: Boolean, outputBucket: String): ZIO[Any, Throwable, Unit]
    def sample(bucket: String, count: Int): ZIO[Any, Throwable, Unit]

    def report: ZIO[Any, Throwable, Unit]

    def resetState: ZIO[Any, Throwable, Unit]

object S3Transfer:

    def layer = ZLayer {
        for {
            dynamoRepository <- ZIO.service[DynamoRepository]
            s3Repository <- ZIO.service[S3Repository]
            _ <- ZIO.unit
        } yield S3TransferLive(s3Repository, dynamoRepository)
    }

case class S3TransferLive(s3: S3Repository, dynamo: DynamoRepository) extends S3Transfer:

    def setup = dynamo.setup
    def transfer(bucket: String, outputBucket: String) = ZIO.unit

    def scan(bucket: String) = for { 
        stream <- dynamo.write(s3.allDocuments(bucket))
    } yield ()

    def copyDocument(doc: S3Document, outputBucket: String) = 
        s3.copy("demo", doc.key, outputBucket, doc.key) *> dynamo.markCopied(doc.key)
        .fold( throwable => s"failure copying ${doc.key} ${throwable.toString()}", _ => s"succes copying ${doc.key}")
    
    def copy(incremental: Boolean, outputBucket: String) = for {
        _ <- s3.createBucket(outputBucket)
        stream <- dynamo.allS3Documents(incremental)
        _ <- stream.filter(doc => doc.processed.isEmpty || !incremental)
             .tap(doc => Console.printLine(doc.toString))
             .mapZIOParUnordered(32)(copyDocument(_, outputBucket))
             .foreach(s => Console.printLine(s))
    } yield ()
    def sample(bucket: String, count: Int) = s3.sample(bucket, count)

    def report: ZIO[Any, Throwable, Unit] = dynamo.report

    def resetState: ZIO[Any, Throwable, Unit] = dynamo.resetState
    
