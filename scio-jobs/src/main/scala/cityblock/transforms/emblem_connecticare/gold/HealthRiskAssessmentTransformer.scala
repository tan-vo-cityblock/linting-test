package cityblock.transforms.emblem_connecticare.gold

import cityblock.models.EmblemSilverClaims.SilverHealthRiskAssessment
import cityblock.models.Identifier
import cityblock.models.emblem.gold.HealthRiskAssessment.MappedHRA
import cityblock.models.gold.Claims.MemberIdentifier
import cityblock.transforms.Transform
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

object HealthRiskAssessmentTransformer {
  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)

    val bigqueryProject: String = args.required("bigqueryProject")
    val dtf = DateTimeFormat.forPattern(Transform.ShardNamePattern)
    val deliveryDate: LocalDate = LocalDate.parse(args.required("deliveryDate"), dtf)

    val hras: SCollection[SilverHealthRiskAssessment] =
      Transform.fetchFromBigQuery[SilverHealthRiskAssessment](
        sc,
        bigqueryProject,
        "silver_claims",
        "Health_Risk_Assessment",
        deliveryDate
      )

    val goldHRAS = hras.map(hra => mapHRAResponses(hra, bigqueryProject))

    Transform.persist(
      goldHRAS,
      bigqueryProject,
      "gold_claims",
      "HealthRiskAssessment",
      deliveryDate,
      WRITE_TRUNCATE,
      CREATE_IF_NEEDED
    )

    sc.close().waitUntilFinish()
  }

  // scalastyle:off method.length
  def mapHRAResponses(healthRA: SilverHealthRiskAssessment, bigqueryProject: String): MappedHRA = {
    val hra = healthRA.data
    MappedHRA(
      identifier = identifier(healthRA, bigqueryProject),
      memberIdentifier = memberIdentifier(healthRA, bigqueryProject),
      BATCHID = hra.BATCHID,
      LINEOFBUSINESS = hra.LINEOFBUSINESS.flatMap(validateLOB),
      MEMBERNUMBER = hra.MEMBERNUMBER,
      FIRSTNAME = hra.FIRSTNAME,
      LASTNAME = hra.LASTNAME,
      PHONENUMBER = hra.PHONENUMBER,
      GENDER = hra.GENDER,
      BIRTHDATE = hra.BIRTHDATE,
      STREETADDRESS = hra.STREETADDRESS,
      STREETADDRESS2 = hra.STREETADDRESS2,
      CITY = hra.CITY,
      STATE = hra.STATE,
      ZIPCODE = hra.ZIPCODE,
      PRODUCTID = hra.PRODUCTID,
      PORTALFILTER1 = hra.PORTALFILTER1,
      PORTALFILTER2 = hra.PORTALFILTER2,
      PBP = hra.PBP,
      MEMBERTYPE = hra.MEMBERTYPE,
      ENROLLDATE = hra.ENROLLDATE,
      UNIQUERECORDID = hra.UNIQUERECORDID,
      CONSENT_TO_CALL = hra.CONSENT_TO_CALL.flatMap(parseBoolean),
      GROUPHOME = hra.GROUPHOME.flatMap(parseBoolean),
      UPDATEDPHONE = hra.UPDATEDPHONE,
      NIDFIRSTLASTNAME = hra.NIDFIRSTLASTNAME,
      NIDDOB = hra.NIDDOB,
      ELIZAENTITYID = hra.ELIZAENTITYID,
      PIN = hra.PIN,
      WELCOMECALLDATE = hra.WELCOMECALLDATE,
      HRAMAILDATETIME = hra.HRAMAILDATETIME,
      HRARESPONDDATETIME = hra.HRARESPONDDATETIME,
      HRARESPONDMETHOD = hra.HRARESPONDMETHOD,
      RESPONDER = hra.RESPONDER.flatMap(mapResponder),
      RESPONDERSTATUS = hra.RESPONDERSTATUS,
      CONTACTSUCCESS = hra.CONTACTSUCCESS.flatMap(parseBoolean),
      INTERVIEWAGREEMENT = hra.INTERVIEWAGREEMENT.flatMap(parseBoolean),
      NOTCONTACTEDREASON = hra.NOTCONTACTEDREASON.flatMap(mapNotContactedReason),
      PROGRAM1SCRUBDATE = hra.PROGRAM1SCRUBDATE,
      PROGRAM1SCRUBREASON = hra.PROGRAM1SCRUBREASON,
      PROGRAM2SCRUBDATE = hra.PROGRAM2SCRUBDATE,
      PROGRAM2SCRUBREASON = hra.PROGRAM2SCRUBREASON,
      ENROLLDIRECTMAILONLY = hra.ENROLLDIRECTMAILONLY,
      LBLCRESULT = hra.LBLCRESULT,
      CLOSEDDATE = hra.CLOSEDDATE,
      CLOSEDREASON = hra.CLOSEDREASON,
      LIVEAGENTDNCREQUEST = hra.LIVEAGENTDNCREQUEST,
      NOTOCONTINUE = hra.NOTOCONTINUE,
      ENGLISH = hra.ENGLISH,
      SPANISH = hra.SPANISH,
      MANDARIN = hra.MANDARIN,
      PROGRAM1_IVR1_ATTEMPT1_DATE = hra.PROGRAM1_IVR1_ATTEMPT1_DATE,
      PROGRAM1_IVR1_ATTEMPT1_RESULT = hra.PROGRAM1_IVR1_ATTEMPT1_RESULT,
      PROGRAM1_IVR1_ATTEMPT2_DATE = hra.PROGRAM1_IVR1_ATTEMPT2_DATE,
      PROGRAM1_IVR1_ATTEMPT2_RESULT = hra.PROGRAM1_IVR1_ATTEMPT2_RESULT,
      PROGRAM1_IVR1_ATTEMPT3_DATE = hra.PROGRAM1_IVR1_ATTEMPT3_DATE,
      PROGRAM1_IVR1_ATTEMPT3_RESULT = hra.PROGRAM1_IVR1_ATTEMPT3_RESULT,
      PROGRAM1_IVR1_ATTEMPT4_DATE = hra.PROGRAM1_IVR1_ATTEMPT4_DATE,
      PROGRAM1_IVR1_ATTEMPT4_RESULT = hra.PROGRAM1_IVR1_ATTEMPT4_RESULT,
      PROGRAM1_IVR1_ATTEMPT5_DATE = hra.PROGRAM1_IVR1_ATTEMPT5_DATE,
      PROGRAM1_IVR1_ATTEMPT5_RESULT = hra.PROGRAM1_IVR1_ATTEMPT5_RESULT,
      PROGRAM1_IVR1_ATTEMPT6_DATE = hra.PROGRAM1_IVR1_ATTEMPT6_DATE,
      PROGRAM1_IVR1_ATTEMPT6_RESULT = hra.PROGRAM1_IVR1_ATTEMPT6_RESULT,
      PROGRAM1_IVR1_ATTEMPT7_DATE = hra.PROGRAM1_IVR1_ATTEMPT7_DATE,
      PROGRAM1_IVR1_ATTEMPT7_RESULT = hra.PROGRAM1_IVR1_ATTEMPT7_RESULT,
      PROGRAM1_DMHRA1_MAILED_DATE = hra.PROGRAM1_DMHRA1_MAILED_DATE,
      PROGRAM1_DMHRA1_MAILED_RESULT = hra.PROGRAM1_DMHRA1_MAILED_RESULT,
      PROGRAM1_DMHRA1_RETURNED_DATE = hra.PROGRAM1_DMHRA1_RETURNED_DATE,
      PROGRAM1_DMHRA1_RETURNED_RESULT = hra.PROGRAM1_DMHRA1_RETURNED_RESULT,
      PROGRAM1_LA_ATTEMPT1_DATE = hra.PROGRAM1_LA_ATTEMPT1_DATE,
      PROGRAM1_LA_ATTEMPT1_RESULT = hra.PROGRAM_1_LA_ATTEMPT1_RESULT,
      PROGRAM1_LA_ATTEMPT2_DATE = hra.PROGRAM1_LA_ATTEMPT2_DATE,
      PROGRAM1_LA_ATTEMPT2_RESULT = hra.PROGRAM_1_LA_ATTEMPT4_RESULT,
      PROGRAM1_LA_ATTEMPT3_DATE = hra.PROGRAM1_LA_ATTEMPT3_DATE,
      PROGRAM1_LA_ATTEMPT3_RESULT = hra.PROGRAM_1_LA_ATTEMPT4_RESULT,
      PROGRAM1_LA_ATTEMPT4_DATE = hra.PROGRAM1_LA_ATTEMPT4_DATE,
      PROGRAM1_LA_ATTEMPT4_RESULT = hra.PROGRAM_1_LA_ATTEMPT4_RESULT,
      PROGRAM1_DM2_DATE = hra.PROGRAM_1_DM2_DATE,
      PROGRAM1_DM2_RESULT = hra.PROGRAM_1_DM2_RESULT,
      PROGRAM1_DM3_DATE = hra.PROGRAM_1_DM3_DATE,
      PROGRAM1_DM3_RESULT = hra.PROGRAM_1_DM3_RESULT,
      PROGRAM1_IVR2_DATE = hra.PROGRAM_1_IVR_2_DATE,
      PROGRAM1_IVR2_RESULT = hra.PROGRAM_1_IVR_2_RESULT,
      PROGRAM2_IVR3_DATE = hra.PROGRAM_2_IVR_3_DATE,
      PROGRAM2_IVR3_RESULT = hra.PROGRAM_2_IVR_3_RESULT,
      PROGRAM2_DMHRA4_MAILED_DATE = hra.PROGRAM2_DMHRA4_MAILED_DATE,
      PROGRAM2_DMHRA4_MAILED_RESULT = hra.PROGRAM2_DMHRA4_MAILED_RESULT,
      PROGRAM2_DMHRA4_RETURNED_DATE = hra.PROGRAM2_DMHRA4_RETURNED_DATE,
      PROGRAM2_DMHRA4_RETURNED_RESULT = hra.PROGRAM2_DMHRA4_MAILED_RESULT,
      PROGRAM2_DMHRA5_MAILED_DATE = hra.PROGRAM2_DMHRA5_MAILED_DATE,
      PROGRAM2_DMHRA5_MAILED_RESULT = hra.PROGRAM2_DMHRA5_MAILED_RESULT,
      PROGRAM2_DMHRA5_RETURNED_DATE = hra.PROGRAM2_DMHRA5_RETURNED_DATE,
      PROGRAM2_DMHRA5_RETURNED_RESULT = hra.PROGRAM2_DMHRA5_RETURNED_RESULT,
      PROGRAM2_DM6_DATE = hra.PROGRAM_2_DM6_DATE,
      PROGRAM2_DM6_RESULT = hra.PROGRAM_2_DM6_RESULT,
      SPANISH_MANDARIN_DTMF = hra.SPANISH_MANDARIN_DTMF,
      CONTINUE = hra.CONTINUE,
      REPEAT_NTC = hra.REPEAT_NTC,
      VALIDATE_DOB = hra.VALIDATE_DOB,
      REPEAT_DOCTOR = hra.REPEAT_DOCTOR,
      INTENT_VISIT = hra.INTENT_VISIT,
      CURRENTLY_PREGNANT = hra.CURRENTLY_PREGNANT.flatMap(parseBoolean),
      INVOLVEMENT_OF_THIRD_PARTY_FOR_MEMBER_HA = hra.HELP_SURVEY.flatMap(mapHelpSurvey),
      AVAILABLE_NOW = hra.AVAILABLE_NOW,
      WAIT_HELP = hra.WAIT_HELP,
      ARE_YOU_READY = hra.ARE_YOU_READY,
      PERSON_HELPING_NAME = hra.GATHER_HELP_NAME,
      OVERALL_HEALTH_PERCEPTION = hra.OVERALL_HEALTH.flatMap(mapRating),
      OVERNIGHT_HOSPITAL = hra.OVERNIGHT_HOSPITAL,
      FREQUENCY_OF_HOSPITALIZATIONS = hra.NUMBER_VISIT_OVERNIGHT.flatMap(mapEventFrequency),
      USED_URGENT_CARE = hra.TIMES_URGENT_CARE,
      FREQUENCY_OF_ER_VISTS_AND_ALTERNATIVE_CARE_SITES =
        hra.TIMES_URGENT_CARE.flatMap(mapEventFrequency),
      USED_TOBACCO = hra.USED_TOBACCO,
      TOBACCO_TYPE = hra.TYPE_TOBACCO.flatMap(mapTobaccoType),
      HEAVY_DRINKING_FOR_MEN = hra.ALCOHOL_BINGE_MALE.flatMap(mapEventFrequency),
      HEAVY_DRINKING_FOR_WOMEN = hra.ALCOHOL_BINGE_FEMALE.flatMap(mapEventFrequency),
      USE_OF_RECREATIONAL_DRUGS = hra.USE_DRUGS.flatMap(parseBoolean),
      FEELING_DEPRESSED = hra.FELT_DEPRESSED.flatMap(mapFrequency),
      LOSS_OF_INTEREST = hra.LACKED_INTEREST.flatMap(mapFrequency),
      HAVE_ASTHMA = mapDefined(hra.HAVE_ASTHMA),
      HAVE_DIABETES = mapDefined(hra.HAVE_DIABETES),
      HAVE_PREDIABETES = mapDefined(hra.HAVE_PREDIABETES),
      HAVE_HF = mapDefined(hra.HAVE_HF),
      HAVE_CAD = mapDefined(hra.HAVE_CAD),
      HAVE_HEART_ATTACK = mapDefined(hra.HAVE_HEART_ATTACK),
      HAVE_HEART_DISEASE = mapDefined(hra.HAVE_HEART_DISEASE),
      HAVE_COPD = mapDefined(hra.HAVE_COPD),
      HAVE_CANCER = mapDefined(hra.HAVE_CANCER),
      HAVE_HIV_AIDS = mapDefined(hra.HAVE_HIV_AIDS),
      HAVE_BACK_PAIN = mapDefined(hra.HAVE_BACK_PAIN),
      HAVE_DEPRESSION = mapDefined(hra.HAVE_DEPRESSION),
      HAVE_SCIZOPHRENIA = mapDefined(hra.HAVE_SCIZOPHRENIA),
      HAVE_BIPOLAR = mapDefined(hra.HAVE_BIPOLAR),
      HAD_STROKE = mapDefined(hra.HAD_STROKE),
      HAVE_KIDNEY_DISEASE = mapDefined(hra.HAVE_KIDNEY_DISEASE),
      HAVE_ARTHRITIS = mapDefined(hra.HAVE_ARTHRITIS),
      HAVE_ANGINA = mapDefined(hra.HAVE_ANGINA),
      HAVE_ULCER = mapDefined(hra.HAVE_ULCER),
      HAVE_OTHER_CONDITION = mapDefined(hra.HAVE_OTHER_CONDITION),
      NO_CONDITION = mapDefined(hra.NO_CONDITION),
      PARTIAL_OR_FULL_BLIND = hra.PROBLEMS_VISION.flatMap(mapVisionProblems),
      VISION_EQUIPMENT = hra.VISION_EQUIPMENT,
      VISION_EQUIPMENT_TYPE = hra.VISION_EQUIP_TYPE.flatMap(mapVisionEquipmentType),
      PROBLEMS_HEARING = hra.PROBLEMS_HEARING,
      HEARING_EQUIPMENT = hra.HEARING_EQUIPMENT,
      HEARING_EQUIPMENT_TYPE = hra.HEARING_EQUIP_TYPE.flatMap(mapHearingEquipmentType),
      TAKING_MEDICATIONS = hra.TAKING_MEDICATIONS,
      RX_NUMBER_ON_DAILY_OR_QOD_BASIS = hra.NUMBER_MEDICATION.flatMap(mapNumberMedications),
      DAILY_ACTIVITY = hra.DAILY_ACTIVITY.flatMap(mapFrequency),
      HELP_BATHING = mapDefined(hra.HELP_BATHING),
      HELP_DRESSING = mapDefined(hra.HELP_DRESSING),
      HELP_GROOMING = mapDefined(hra.HELP_GROOMING),
      HELP_TOILET = mapDefined(hra.HELP_TOILET),
      HELP_EATING = mapDefined(hra.HELP_EATING),
      HELP_MEALS = mapDefined(hra.HELP_MEALS),
      HELP_SHOPPING = mapDefined(hra.HELP_SHOPPING),
      HELP_WALKING = mapDefined(hra.HELP_WALKING),
      HELP_STAIRS = mapDefined(hra.HELP_STAIRS),
      HELP_TRANSFERRING = mapDefined(hra.HELP_TRANSFERRING),
      HELP_HOUSEKEEPING = mapDefined(hra.HELP_HOUSEKEEPING),
      HELP_DRIVING = mapDefined(hra.HELP_DRIVING),
      HELP_ERRANDS = mapDefined(hra.HELP_ERRANDS),
      HELP_BILLS = mapDefined(hra.HELP_BILLS),
      HELP_LAUNDRY = mapDefined(hra.HELP_LAUNDRY),
      HELP_PHONE = mapDefined(hra.HELP_PHONE),
      HELP_MEDICATION = mapDefined(hra.HELP_MEDICATION),
      HELP_OTHER_ACTIVITY = mapDefined(hra.HELP_OTHER_ACTIVITY),
      ASSISTANCE_WITH_ADLS_BESIDES_HHC = hra.HOME_CARE,
      CARE_PERSON = hra.CARE_PERSON.flatMap(mapCarePerson),
      CAREGIVER_ASSIST = hra.HAVE_CAREGIVER.flatMap(mapHaveCaregiver),
      HAVE_DENTIST = hra.HAVE_DENTIST,
      DENTAL_VISIT_FREQUENCY = hra.FREQUENCY_DENTIST.flatMap(mapDentistFrequency),
      VISIT_PCP = hra.VISIT_PCP,
      TRANSPORTATION_PROBLEM = mapDefined(hra.TRANSPORTATION_PROBLEM),
      HOUSING_CONCERN = mapDefined(hra.HOUSING_CONCERN),
      CLOTHING_CONCERN = mapDefined(hra.CLOTHING_CONCERN),
      FOOD_CONCERN = mapDefined(hra.FOOD_CONCERN),
      FINANCES_BARRIER = mapDefined(hra.FINANCES_BARRIER),
      EMPLOYMENT_BARRIER = mapDefined(hra.EMPLOYMENT_BARRIER),
      GET_SOCIAL_SUPPORT = mapDefined(hra.GET_SOCIAL_SUPPORT),
      LANGUAGE_BARRIER = mapDefined(hra.LANGUAGE_BARRIER),
      CULTURAL_BARRIER = mapDefined(hra.CULTURAL_BARRIER),
      MOTIVATION_BARRIER = mapDefined(hra.MOTIVATION_BARRIER),
      STRESS_BARRIER = mapDefined(hra.STRESS_BARRIER),
      CAREGIVER_BARRIER = mapDefined(hra.CAREGIVER_BARRIER),
      CRIME_EXPOSURE = mapDefined(hra.CRIME_EXPOSURE),
      RESIDENTIAL_ISOLATION = mapDefined(hra.RESIDENTIAL_ISOLATION),
      TECHNOLOGY_BARRIER = mapDefined(hra.TECHNOLOGY_BARRIER),
      OTHER_BARRIER = mapDefined(hra.OTHER_BARRIER),
      ADVANCED_DIRECTIVE = hra.ADVANCED_DIRECTIVE.flatMap(parseBoolean),
      PREFERRED_LANGUAGE_COMMUNICATE = hra.COMMUNICATE_LANGUAGE.flatMap(simpleMapLanguage),
      PREFERRED_LANGUAGE_READING = hra.GATHER_PREFERRED_LANG.flatMap(mapLanguage),
      CONTACT_PREFERENCE = hra.CONTACT_PREFERENCE.map(mapContactPreference),
      CORRECT_ADDRESS = hra.CORRECT_ADDRESS,
      GATHER_ADDRESS = hra.GATHER_ADDRESS,
      CALL_HELPFUL = hra.CALL_HELPFUL
    )

  }

  def identifier(silver: SilverHealthRiskAssessment, project: String): Identifier =
    Identifier(
      id = silver.data.UNIQUERECORDID,
      partner = project.split("-").head,
      surrogate = Transform
        .addSurrogate(
          project,
          "silver_claims",
          "Health_Risk_Assessment",
          silver
        )(_.identifier.surrogateId)
        ._1
    )

  def memberIdentifier(silver: SilverHealthRiskAssessment, project: String): MemberIdentifier =
    MemberIdentifier(
      commonId = silver.patient.source.commonId,
      partnerMemberId = silver.patient.externalId,
      patientId = silver.patient.patientId,
      partner = project.split("-").head
    )

  def mapRating(rating: String): Option[String] =
    rating match {
      case "1" => Some("Excellent")
      case "2" => Some("Good")
      case "3" => Some("Fair")
      case "4" => Some("Poor")
      case "5" => Some("Very poor")
      case _   => None
    }

  def mapVisionEquipmentType(equipment: String): Option[String] =
    equipment match {
      case "1" => Some("Large text")
      case "2" => Some("Magnifying glass/computer screen magnifier")
      case "3" => Some("Special lighting")
      case "4" => Some("Computer adaptations")
      case "5" => Some("Other")
      case _   => None
    }

  def mapHearingEquipmentType(equipment: String): Option[String] =
    equipment match {
      case "1" => Some("Hearing aid")
      case "2" => Some("TDD/TTY service")
      case "3" => Some("Other")
      case "4" => Some("None")
      case _   => None
    }

  def mapNumberMedications(num: String): Option[String] =
    num match {
      case "1" => Some("1-2")
      case "2" => Some("3-4")
      case "3" => Some("5-7")
      case "4" => Some("8-10")
      case "5" => Some("Over 10")
      case _   => None
    }

  def mapCarePerson(carePerson: String): Option[String] =
    carePerson match {
      case "1" => Some("Family member(s)")
      case "2" => Some("Spouse/significant other")
      case "3" => Some("Friend")
      case "4" => Some("Neighbor")
      case "5" => Some("Private service")
      case "6" => Some("Community service")
      case "7" => Some("Religious affilation")
      case "8" => Some("Do not need assistance at this time")
      case _   => None
    }

  def mapHaveCaregiver(haveCaregiver: String): Option[String] =
    haveCaregiver match {
      case "1" => Some("Yes")
      case "2" => Some("No, but I'm not in need of any help right now")
      case "3" => Some("No, but I feel in need of someone to help me")
      case _   => None
    }

  def mapVisionProblems(vision: String): Option[String] =
    vision match {
      case "1" => Some("Yes, partially blind")
      case "2" => Some("Yes, fully blind")
      case "3" => Some("No")
      case _   => None
    }

  def mapFrequency(frequency: String): Option[String] =
    frequency match {
      case "1" => Some("Always")
      case "2" => Some("Often")
      case "3" => Some("Sometimes")
      case "4" => Some("Rarely")
      case "5" => Some("Never")
      case _   => None
    }

  def mapEventFrequency(frequency: String): Option[String] =
    frequency match {
      case "0" => Some("None")
      case "1" => Some("One time")
      case "2" => Some("Two times")
      case "3" => Some("Three times")
      case "4" => Some("More than three times")
      case "5" => Some("I don't drink alcohol")
      case _   => None
    }

  def mapHelpSurvey(help: String): Option[String] =
    help match {
      case "1" => Some("Yes")
      case "2" => Some("Yes, but they are not available right now")
      case "3" => Some("No")
      case _   => None
    }

  def mapTobaccoType(tobacco: String): Option[String] =
    tobacco match {
      case "1" => Some("Cigarettes")
      case "2" => Some("Cigars")
      case "3" => Some("Pipe tobacco")
      case "4" => Some("Smokeless tobacco")
      case "5" => Some("None")
      case _   => None
    }

  def mapDentistFrequency(frequency: String): Option[String] =
    frequency match {
      case "1" => Some("3-6 months")
      case "2" => Some("6 months to a year")
      case "3" => Some("Don't know")
      case "4" => Some("Less than 3 months")
      case "5" => Some("More than a year")
      case "6" => Some("Never")
      case _   => None
    }

  def simpleMapLanguage(language: String): Option[String] =
    language match {
      case "1" => Some("English")
      case "2" => Some("Spanish")
      case "3" => Some("Other")
      case _   => None
    }

  def mapLanguage(language: String): Option[String] =
    language match {
      case "1"  => Some("English")
      case "2"  => Some("Spanish")
      case "3"  => Some("French")
      case "4"  => Some("German")
      case "5"  => Some("Arabic")
      case "6"  => Some("Hebrew")
      case "7"  => Some("Japanese")
      case "8"  => Some("Italian")
      case "9"  => Some("Polish")
      case "10" => Some("Russian")
      case "11" => Some("Chinese")
      case "12" => Some("Other")
      case _    => None
    }

  def mapResponder(responder: String): Option[String] =
    responder match {
      case "1" => Some("Member")
      case "2" => Some("Caregiver")
      case "3" => Some("Not Available")
      case _   => None
    }

  def validateLOB(lob: String): Option[String] = {
    val validLOBs = Set("EHMR", "EHSNP", "AFSNP", "AFMR", "CCIMEDICARE", "CCIDSNP")
    if (!validLOBs.contains(lob)) None else Some(lob)
  }

  def parseBoolean(bool: String): Option[String] =
    bool match {
      case "Y" | "1" => Some("Yes")
      case "N" | "2" => Some("No")
      case "3"       => Some("No answer")
      case _         => None
    }

  def mapNotContactedReason(reason: String): Option[String] =
    reason match {
      case "1" => Some("Busy signal")
      case "2" => Some("No answer")
      case "3" => Some("Hung up")
      case "4" => Some("Left voicemail message")
      case "5" => Some("Wrong number")
      case "6" => Some("Disconnected")
      case "7" => Some("Mail only program")
      case _   => None
    }

  def mapContactPreference(preferenceList: String): String = {
    val preferences: Array[String] = preferenceList.split(",")

    preferences
      .flatMap {
        case "1" => Some("Phone")
        case "2" => Some("Postal mail")
        case "3" => Some("Email")
        case "4" => Some("Text")
        case "5" => Some("Web portal")
        case "6" => Some("Other")
        case _   => None
      }
      .mkString(", ")
  }

  def mapDefined(answer: Option[String]): Option[String] =
    answer match {
      case Some(_) => Some("Yes")
      case None    => Some("No")
    }
}
