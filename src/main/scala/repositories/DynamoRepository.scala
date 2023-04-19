package repositories 

import zio._
import zio.stream._
import zio.dynamodb.DynamoDBExecutor
import zio.dynamodb.DynamoDBQuery.{ createTable, describeTable, put, queryAll, scanAll, scanAllItem }
import zio.dynamodb._
import zio.Exit.Success
import zio.Exit.Failure
import model.S3Document

trait DynamoRepository:

    def setup: ZIO[Any, Throwable, Unit]

    def write(stream: ZStream[Any, Throwable, S3Document]): ZIO[Any, Throwable, Unit]

    def report: ZIO[Any, Throwable, Unit]

    def allS3Documents: ZIO[Any, Throwable, ZStream[Any, Throwable, S3Document]]

object DynamoRepository:

    def layer(dynamodbTable: String): ZLayer[DynamoDBExecutor, Unit, DynamoRepository] = ZLayer {
        for {
            dynamodbExecutor <- ZIO.service[DynamoDBExecutor]
        } yield DynamoRepositoryLive(dynamodbExecutor, dynamodbTable)
    }

    case class Report(items: Long, processedItems: Long)


case class DynamoRepositoryLive(dynamodbExecutor: DynamoDBExecutor, dynamodbTable: String) extends DynamoRepository: 
    self =>

    def create(dynamodbTable: String): ZIO[DynamoDBExecutor, Throwable, String] = createTable(
        dynamodbTable, 
        KeySchema("key"), 
        BillingMode.PayPerRequest
    )(AttributeDefinition.attrDefnString("key")).execute *> ZIO.succeed(s"$dynamodbTable created")

    def createTableIfNotExist(dynamodbTable: String): ZIO[DynamoDBExecutor, Throwable, String] = 
        describeTable(dynamodbTable).execute.map(d => s"${d.tableArn} already exists")
        .orElse(create(dynamodbTable)) 
        
    def setup: ZIO[Any, Throwable, Unit] =  (for { 
        description <- createTableIfNotExist(dynamodbTable)
        _ <- ZIO.log(description)
    } yield ()).provideEnvironment(ZEnvironment(dynamodbExecutor))

    def failIfNotExist = describeTable(dynamodbTable).execute.orElse(ZIO.fail(new Exception(s"$dynamodbTable does not exist")))

    def write(stream: ZStream[Any, Throwable, S3Document]): ZIO[Any, Throwable, Unit] = for {
        _ <- ZIO.log(dynamodbTable)
        _ <- batchWriteFromStream(stream) { s3Document => 
            put(dynamodbTable, s3Document)
         }.runDrain.provideEnvironment(ZEnvironment(dynamodbExecutor))
        } yield ()

    val computeReport = ZSink.foldLeft[S3Document, DynamoRepository.Report](DynamoRepository.Report(0, 0)) { (report, doc) => 
        DynamoRepository.Report(report.items + 1, report.processedItems + doc.processed.map(_ => 1).getOrElse(0))
    }

    def allS3Documents = 
        implicit val sch = S3Document.schema
        (for {
            stream <- scanAllItem(dynamodbTable, S3Document.key, S3Document.creationDate, S3Document.processed).execute.provideEnvironment(ZEnvironment(dynamodbExecutor))
            stream2 = stream.mapZIO(item => ZIO.fromEither(item.get[String]("key")))
            stream3 = batchReadFromStream(dynamodbTable, stream2)(pk => S3Document.primaryKey(pk))
        } yield stream3.map(_._2).provideEnvironment(ZEnvironment(dynamodbExecutor)))

    def report =
        (for {
        stream <- allS3Documents
        report <- stream.run(computeReport)
        _ <- Console.printLine(report)
    } yield ()).provideEnvironment(ZEnvironment(dynamodbExecutor))
