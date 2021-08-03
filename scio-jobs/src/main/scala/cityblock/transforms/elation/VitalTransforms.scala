package cityblock.transforms.elation

import java.time.{Instant => JavaInstant}

import scala.util.control.NonFatal
import cityblock.ehrmodels.elation.datamodelapi.vital.{
  VitalContent,
  VitalContentBP,
  Vital => ElationVital
}
import cityblock.member.service.models.PatientInfo.Patient
import cityblock.streaming.jobs.aggregations.VitalSigns._
import cityblock.utilities.{Conversions, Loggable, Loinc}
import cityblock.utilities.Conversions._
import cityblock.utilities.time.DateOrInstant

/**
 * Ojbect is used to transform Elation Vital objects to PatientVitalSign objects that are persisted to BQ and GCS.
 * Per Elation API docs (https://docs.elationhealth.com/reference#vitals):
 * most vital measurements come in a json array of size 0 0r 1 so we can safely grab head of list.
 *
 * For blood pressure vitals, the number of measurements is not restricted and can range from 0 t0 n. For simplicity,
 * we are currently grabbing the head.
 */
object VitalTransforms extends Loggable {
  def transformElationVitalsToPatientVitals(
    messageId: String,
    patient: Patient,
    elationVitals: List[ElationVital]
  ): List[PatientVitalSign] = {
    elationVitals.flatMap { elationVital =>
      val bmi: List[PatientVitalSign] = getBMIVital(
        elationVital.bmi,
        elationVital.signed_date,
        messageId,
        patient
      )

      val height: List[PatientVitalSign] =
        getStandardVital(
          elationVital.height,
          elationVital.signed_date,
          Loinc.VitalSign.Height,
          messageId,
          patient
        )

      val weight: List[PatientVitalSign] =
        getStandardVital(
          elationVital.weight,
          elationVital.signed_date,
          Loinc.VitalSign.Weight,
          messageId,
          patient
        )

      val oxygen: List[PatientVitalSign] =
        getStandardVital(
          elationVital.oxygen,
          elationVital.signed_date,
          Loinc.VitalSign.Oxygen,
          messageId,
          patient
        )

      val respiratoryRate: List[PatientVitalSign] =
        getStandardVital(
          elationVital.rr,
          elationVital.signed_date,
          Loinc.VitalSign.RespiratoryRate,
          messageId,
          patient
        )

      val heartRate: List[PatientVitalSign] =
        getStandardVital(
          elationVital.hr,
          elationVital.signed_date,
          Loinc.VitalSign.HeartRate,
          messageId,
          patient
        )

      val headCircum: List[PatientVitalSign] =
        getStandardVital(
          elationVital.hc,
          elationVital.signed_date,
          Loinc.VitalSign.HeadCircum,
          messageId,
          patient
        )

      val temperature: List[PatientVitalSign] =
        getStandardVital(
          elationVital.temperature,
          elationVital.signed_date,
          Loinc.VitalSign.Temperature,
          messageId,
          patient
        )

      val bloodPressure: List[PatientVitalSign] = getBloodPressureVitals(
        elationVital.bp,
        elationVital.signed_date,
        messageId,
        patient
      )

      val ketone: List[PatientVitalSign] = getOptionalVital(
        elationVital.ketone,
        elationVital.signed_date,
        messageId,
        patient
      )

      val bodyFat: List[PatientVitalSign] = getOptionalVital(
        elationVital.bodyfat,
        elationVital.signed_date,
        messageId,
        patient
      )

      val dryLeanMass: List[PatientVitalSign] = getOptionalVital(
        elationVital.dlm,
        elationVital.signed_date,
        messageId,
        patient
      )

      val bodyFatMass: List[PatientVitalSign] = getOptionalVital(
        elationVital.bfm,
        elationVital.signed_date,
        messageId,
        patient
      )

      val waistCircum: List[PatientVitalSign] = getOptionalVital(
        elationVital.wc,
        elationVital.signed_date,
        messageId,
        patient
      )

      List(
        bmi,
        height,
        weight,
        oxygen,
        respiratoryRate,
        heartRate,
        headCircum,
        temperature,
        bloodPressure,
        ketone,
        bodyFat,
        dryLeanMass,
        bodyFatMass,
        waistCircum
      ).flatten
    }
  }

  private def getBMIVital(
    bmiElationVital: Option[Double],
    signedDate: Option[JavaInstant],
    messageId: String,
    patient: Patient
  ): List[PatientVitalSign] =
    bmiElationVital match {
      case None =>
        List()

      case Some(bmiVitalValue) =>
        val vitalSign = getVitalSignFromTemplate(
          Loinc.VitalSign.BMI.code,
          Loinc.VitalSign.BMI.name,
          signedDate,
          bmiVitalValue.toString,
          Loinc.VitalSign.BMI.unit
        )
        List(PatientVitalSign(messageId, patient, vitalSign))
    }

  private def getStandardVital(
    vitalContentList: List[VitalContent],
    signedDate: Option[JavaInstant],
    loincVital: Loinc.VitalSign.Value,
    messageId: String,
    patient: Patient
  ): List[PatientVitalSign] =
    vitalContentList match {
      case vitalContent :: _ if vitalContent.value.trim.nonEmpty =>
        val (convertedVal, convertedUnits) = convertVitalValueIfNeeded(
          vitalContent.value,
          vitalContent.units.getOrElse("Unknown"),
          loincVital
        )

        val vitalSign = getVitalSignFromTemplate(
          loincVital.code,
          loincVital.name,
          signedDate,
          convertedVal,
          convertedUnits
        )
        List(PatientVitalSign(messageId, patient, vitalSign))

      case _ => List()
    }

  private def getBloodPressureVitals(
    bpVitalContent: List[VitalContentBP],
    signedDate: Option[JavaInstant],
    messageId: String,
    patient: Patient
  ): List[PatientVitalSign] =
    bpVitalContent match {
      case vitalContentBP :: _
          if vitalContentBP.diastolic.trim.nonEmpty && vitalContentBP.systolic.trim.nonEmpty =>
        val vitalSignSys = getVitalSignFromTemplate(
          Loinc.VitalSign.SystolicBP.code,
          Loinc.VitalSign.SystolicBP.name,
          signedDate,
          vitalContentBP.systolic,
          Loinc.VitalSign.SystolicBP.unit
        )

        val vitalSignDias = getVitalSignFromTemplate(
          Loinc.VitalSign.DiastolicBP.code,
          Loinc.VitalSign.DiastolicBP.name,
          signedDate,
          vitalContentBP.diastolic,
          Loinc.VitalSign.DiastolicBP.unit
        )

        List(
          PatientVitalSign(messageId, patient, vitalSignSys),
          PatientVitalSign(messageId, patient, vitalSignDias)
        )

      case _ => List()
    }

  private def getOptionalVital(
    optVitalContentList: Option[List[VitalContent]],
    signedDate: Option[JavaInstant],
    messageId: String,
    patient: Patient
  ): List[PatientVitalSign] =
    optVitalContentList match {
      case None => List()

      case Some(vitalContentList) =>
        vitalContentList match {
          case vitalContent :: _ if vitalContent.value.trim.nonEmpty =>
            val vitalSign = getVitalSignFromTemplate(
              "Unknown",
              vitalContent.label.getOrElse("Unknown"),
              signedDate,
              vitalContent.value,
              vitalContent.units.getOrElse("Unknown")
            )
            List(PatientVitalSign(messageId, patient, vitalSign))
          case _ => List()
        }
    }

  private def getVitalSignFromTemplate(
    code: String,
    name: String,
    signedDate: Option[JavaInstant],
    value: String,
    units: String
  ): VitalSign = {
    val (completedDate: Option[DateOrInstant], status: String) =
      signedDate match {
        case Some(date) =>
          (Some(Conversions.mkDateOrInstant(date)), "completed")
        case None =>
          (None, "incomplete")
      }

    VitalSign(
      Code = code,
      CodeSystem = Loinc.codeSystem,
      CodeSystemName = Loinc.codeSystemName,
      Name = name,
      Status = status,
      Interpretation = "",
      DateTime = completedDate,
      Value = value,
      Units = units
    )
  }

  private def convertVitalValueIfNeeded(
    value: String,
    elationUnit: String,
    loincVital: Loinc.VitalSign.Value
  ): (String, String) =
    try {
      elationUnit match {
        case "inches" =>
          (inchesToCm(value.toDouble), loincVital.unit)
        case "lbs" =>
          (lbsToKg(value.toDouble), loincVital.unit)
        case "fahrenheit" =>
          (fahrenheitToCel(value.toDouble), loincVital.unit)
        case "%" =>
          (value.replace("%", ""), loincVital.unit)
        case _ => (value, loincVital.unit)
      }
    } catch {
      case e: NumberFormatException =>
        logger.error(
          s"""Number Format Exception while converting value: [$value], with unit: [$elationUnit] to Double.
             |Exception: ${e.toString}""".stripMargin
        )
        (value, elationUnit)
      case NonFatal(e) =>
        throw new Exception(
          s"""Unexpected Exception while converting value: [$value], with unit: [$elationUnit] to Double.
             |Exception: ${e.toString}""".stripMargin
        )
    }
}
