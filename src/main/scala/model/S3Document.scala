package model

import zio.dynamodb.Annotations.enumOfCaseObjects
import zio.dynamodb.{ PrimaryKey, ProjectionExpression }
import zio.schema.DeriveSchema

import java.time.Instant
import zio.schema.Schema

final case class S3Document(
  key: String,
  creationDate: Option[Instant],
  processingLog: List[String],
  processed: Option[Instant]
)

object S3Document: 

  type Sch = Schema.CaseClass4[
    String,
    Option[Instant],
    List[String],
    Option[Instant],
    S3Document
  ]
  
  implicit val schema: Sch = DeriveSchema.gen[S3Document]

  val (
    key,
    creationDate,
    processingLog,
    processed
   ) = ProjectionExpression.accessors[S3Document]

  def primaryKey(key: String): PrimaryKey = PrimaryKey("key" -> key)



