
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
    final case class Setup() extends Subcommand
    final case class Sample(bucket: String, count: Int) extends Subcommand
    final case class Scan(bucket: String) extends Subcommand
    final case class Transform(incremental: Boolean, transformation: String) extends Subcommand
    final case class Report() extends Subcommand
    final case class ResetState() extends Subcommand
  }

  val defaultDynamoDbTable = "s3transfer"

  val setupHelp: HelpDoc = HelpDoc.p(s"Creates the default dynamodb table `$defaultDynamoDbTable` if not exist")
  val setup =
    Command("setup", Options.none, Args.none).withHelp(setupHelp).map { case _ =>
      Subcommand.Setup()
    }

  val sampleHelp: HelpDoc = HelpDoc.p("Generates sample data into the bucket")
  val sample =
    Command("sample", Options.none, Args.text("bucket") ++ Args.integer).withHelp(sampleHelp).map { case (bucket, count) =>
      Subcommand.Sample(bucket.toString, count.toInt)
    }

  val scanHelp: HelpDoc = HelpDoc.p("Scan S3 Folder and store results in dynamodb table")
  val scan =
    Command("scan", Options.none, Args.text("bucket")).withHelp(scanHelp).map { case bucket =>
      Subcommand.Scan(bucket.toString)
    }

  val reportHelp: HelpDoc = HelpDoc.p("Report transformation statistics on the console")
  val report =
    Command("report", Options.none, Args.none).withHelp(reportHelp).map { case _ =>
      Subcommand.Report()
    }

  val incremental: Options[Boolean] = Options.boolean("incremental").alias("i")
  
  val transformHelp: HelpDoc = HelpDoc.p("Transform files and copy them to S3 output bucket")
  val transform =
    Command("transform", incremental, Args.text("output")).withHelp(transformHelp).map { case (options, arguments) =>
      Subcommand.Transform(options, arguments)
    }

  val resetStateHelp: HelpDoc = HelpDoc.p("Reset copy state to false for all documents")
  val resetState =
    Command("reset-state", Options.none, Args.none).withHelp(resetStateHelp).map { case _ =>
      Subcommand.ResetState()
    }

  val s3transform: Command[Subcommand] =
    Command("s3transform", Options.none, Args.none).subcommands(setup, sample, scan, report, transform, resetState)

  val s3layer = s3.live(Region.EU_WEST_1, AwsBasicCredentials.create("mock-key", "mock-key"), Some(new URI("http://localhost:4566")))

  def cmd(subcommand: Subcommand) = subcommand match {
    case Subcommand.Setup() => ZIO.serviceWithZIO[S3Transfer](_.setup)
    case Subcommand.Sample(bucket, count) => ZIO.serviceWithZIO[S3Transfer](_.sample(bucket, count))
    case Subcommand.Scan(bucket) => ZIO.serviceWithZIO[S3Transfer](_.scan(bucket))
    case Subcommand.Transform(incremental, output) => Console.printLine(s"transform to $output, incremental=$incremental") <* ZIO.serviceWithZIO[S3Transfer](_.copy(incremental, output))
    case Subcommand.Report() => ZIO.serviceWithZIO[S3Transfer](_.report)
    case Subcommand.ResetState() => ZIO.serviceWithZIO[S3Transfer](_.resetState)
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
                    DynamoRepository.layer(defaultDynamoDbTable))
                  )

}
