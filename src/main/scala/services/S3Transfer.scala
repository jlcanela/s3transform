package services

import zio.*
import repositories.DynamoRepository
import repositories.S3Repository

trait S3Transfer:

    def setup: ZIO[Any, Throwable, Unit]

    def transfer(bucket: String, outputBucket: String): ZIO[Any, Throwable, Unit]

    def scan(bucket: String): ZIO[Any, Throwable, Unit]
    def copy(outputBucket: String): ZIO[Any, Throwable, Unit]
    def sample(bucket: String, count: Int): ZIO[Any, Throwable, Unit]

    def report: ZIO[Any, Throwable, Unit]

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

    def copy(outputBucket: String) = for {
        stream <- dynamo.allS3Documents.tap(doc => Console.printLine(doc.toString))
        _ <- stream.runDrain
    } yield stream.drain
    def sample(bucket: String, count: Int) = s3.sample(bucket, count)

    def report: ZIO[Any, Throwable, Unit] = dynamo.report
