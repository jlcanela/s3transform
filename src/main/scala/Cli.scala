
import repositories.{DynamoRepository, S3Repository}

import zio.*
import zio.Console.printLine
import zio.cli.HelpDoc.Span.text
import zio.cli._

import java.nio.file.{Path => JPath}
import zio.Console.ConsoleLive
import zio.dynamodb.examples.dynamodblocal.DynamoDB
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.regions.Region
import java.net.URI
import services.S3Transfer

object Cli extends ZIOCliDefault {
  import java.nio.file.Path

  sealed trait Subcommand extends Product with Serializable
  object Subcommand {
    final case class Setup(dynamodbTable: String) extends Subcommand
    final case class Sample(dynamodbTable: String, bucket: String, count: Int) extends Subcommand
    final case class Scan(bucket: String, dynamodbTable: String) extends Subcommand
    final case class Transform(dynamodbTable: String, transformation: String) extends Subcommand
    final case class Report(dynamodbTable: String, output: String) extends Subcommand
  }

  val setupHelp: HelpDoc = HelpDoc.p("Creates the dynamodb table if it does not exist")
  val setup =
    Command("setup", Options.none, Args.text("dynamodbTable")).withHelp(setupHelp).map { case (dynamodbTable) =>
      Subcommand.Setup(dynamodbTable.toString)
    }

  val sampleHelp: HelpDoc = HelpDoc.p("Generates sample data into the bucket")
  val sample =
    Command("sample", Options.none, Args.text("dynamodbTable") ++ Args.text("bucket") ++ Args.integer).withHelp(sampleHelp).map { case (dynamodbTable, bucket, count) =>
      Subcommand.Sample(dynamodbTable.toString, bucket.toString, count.toInt)
    }

  val scanHelp: HelpDoc = HelpDoc.p("Scan S3 Folder and store results in dynamodb table")
  val scan =
    Command("scan", Options.none, Args.text("bucket") ++ Args.text("table")).withHelp(scanHelp).map { case (bucket, table) =>
      Subcommand.Scan(bucket.toString, table.toString)
    }

  val reportHelp: HelpDoc = HelpDoc.p("Report transformation and exports results to S3 output")
  val report =
    Command("report", Options.none, Args.text("table") ++ Args.text("output")).withHelp(reportHelp).map { case (table, output) =>
      Subcommand.Report(table.toString, output.toString)
    }

  val transformHelp: HelpDoc = HelpDoc.p("Report transformation and exports results to S3 output")
  val transform =
    Command("transform", Options.none, Args.text("dynamodbTable") ++ Args.text("output")).withHelp(transformHelp).map { case (dynamodbTable, output) =>
      Subcommand.Transform(dynamodbTable.toString, output.toString)
    }

  val s3transform: Command[Subcommand] =
    Command("s3transform", Options.none, Args.none).subcommands(setup, sample, scan, report, transform)

  val s3layer = s3.live(Region.EU_WEST_1, AwsBasicCredentials.create("mock-key", "mock-key"), Some(new URI("http://localhost:4566")))

  def cmd(subcommand: Subcommand) = subcommand match {
    case Subcommand.Setup(dynamodbTable) => ZIO.serviceWithZIO[S3Transfer](_.setup)
    case Subcommand.Sample(dynamodbTable, bucket, count) => ZIO.serviceWithZIO[S3Transfer](_.sample(bucket, count))
    case Subcommand.Scan(bucket, dynamodbTable) => ZIO.serviceWithZIO[S3Transfer](_.scan(bucket))
    case Subcommand.Transform(dynamodbTable, output) => ZIO.serviceWithZIO[S3Transfer](_.copy(output))
    case Subcommand.Report(dynamodbTable, output) => ZIO.serviceWithZIO[S3Transfer](_.report)
  }

  val cliApp = CliApp.make(
    name = "S3 Transform",
    version = "1.0.0",
    summary = text("a tool to transform s3 objects"),
    command = s3transform
  )(subcommand => cmd(subcommand).provide(
                    S3Transfer.layer, 
                    S3Repository.layer,
                    s3layer, 
                    DynamoDB.layer, 
                    DynamoRepository.layer("s3transfer"))
                  )

}