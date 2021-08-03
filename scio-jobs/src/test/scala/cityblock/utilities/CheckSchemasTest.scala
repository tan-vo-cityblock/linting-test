package cityblock.utilities

import cityblock.importers.generic.config.CsvFileConfig
import cityblock.models.EmblemSilverClaims.{
  SilverDiagnosisAssociationCohort,
  SilverFacilityClaimCohort,
  SilverMemberDemographicCohort,
  SilverMemberMonth,
  SilverPharmacyClaimCohort,
  SilverProcedureAssociation,
  SilverProfessionalClaimCohort
}
import cityblock.models.TuftsSilverClaims.{Medical, SilverMember, SilverPharmacy, SilverProvider}
import cityblock.models.ConnecticareSilverClaims.{FacetsPharmacyMed, FacetsProvider}
import com.google.api.services.bigquery.model.TableSchema
import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

class CheckSchemasTest extends FlatSpec with Matchers {

  class BigQueryTypeValidator[T <: HasAnnotation: TypeTag: ClassTag](configPath: String,
                                                                     config: CsvFileConfig,
                                                                     caseClassSchema: TableSchema) {
    def validate(): Unit = {
      val dataFieldIndex = if (config.indexJoinField.isDefined) 2 else 1

      val caseClassDataField = caseClassSchema.getFields.get(dataFieldIndex)
      val configDataField = config.tableSchema.getFields.get(dataFieldIndex)

      configDataField.getName shouldBe config.dataFieldName
      caseClassDataField.getName shouldBe config.dataFieldName

      implicitly[ClassTag[T]].toString should s"match $configPath" in {
        import collection.JavaConverters._
        caseClassDataField.getFields.asScala.zip(configDataField.getFields.asScala).foreach {
          case (l, r) =>
            (l.getName, l.getType, l.getMode) shouldBe (r.getName, r.getType, r.getMode)
        }
      }
    }
  }

  object BigQueryTypeValidator {
    def apply[T <: HasAnnotation: TypeTag: ClassTag](configPath: String): BigQueryTypeValidator[T] =
      new BigQueryTypeValidator[T](configPath, parseUnsafe(configPath), BigQueryType[T].schema)

    private def parseUnsafe(configPath: String): CsvFileConfig = {
      import io.circe.parser._

      val configFile = Source.fromFile(configPath)
      val config = parse(configFile.mkString) match {
        case Left(e) => throw e
        case Right(json) =>
          json.as[CsvFileConfig] match {
            case Left(e)       => throw e
            case Right(config) => config
          }
      }

      configFile.close
      config
    }
  }

  BigQueryTypeValidator[SilverProfessionalClaimCohort](
    "../partner_configs/batch/emblem/professional.txt").validate()
  BigQueryTypeValidator[SilverFacilityClaimCohort]("../partner_configs/batch/emblem/facility.txt")
    .validate()
  BigQueryTypeValidator[SilverPharmacyClaimCohort]("../partner_configs/batch/emblem/pharmacy.txt")
    .validate()
  BigQueryTypeValidator[SilverDiagnosisAssociationCohort](
    "../partner_configs/batch/emblem/diagnosis_associations.txt").validate()
  BigQueryTypeValidator[SilverProcedureAssociation](
    "../partner_configs/batch/emblem/procedure_associations.txt").validate()
  BigQueryTypeValidator[SilverMemberDemographicCohort](
    "../partner_configs/batch/emblem/member_demographics.txt").validate()
  BigQueryTypeValidator[SilverMemberMonth]("../partner_configs/batch/emblem/member_month.txt")
    .validate()
  BigQueryTypeValidator[SilverPharmacy]("../partner_configs/batch/tufts/PharmacyClaim.txt")
    .validate()
  BigQueryTypeValidator[Medical]("../partner_configs/batch/tufts/MedicalClaim.txt")
    .validate()
  BigQueryTypeValidator[SilverMember]("../partner_configs/batch/tufts/Member.txt")
    .validate()
  BigQueryTypeValidator[SilverProvider]("../partner_configs/batch/tufts/Provider.txt")
    .validate()
  BigQueryTypeValidator[SilverMember]("../partner_configs/batch/tufts/MemberDaily.txt")
    .validate()
  BigQueryTypeValidator[FacetsPharmacyMed](
    "../partner_configs/batch/connecticare/FacetsPharmacy_med.txt").validate()
  BigQueryTypeValidator[FacetsProvider]("../partner_configs/batch/connecticare/FacetsProvider.txt")
    .validate()
}
