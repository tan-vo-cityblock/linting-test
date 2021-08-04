package cityblock.transforms.connecticare

import cityblock.models.ConnecticareSilverClaims
import cityblock.models.ConnecticareSilverClaims._
import cityblock.models.gold.Claims
import cityblock.models.gold.Claims._
import cityblock.models.gold.FacilityClaim.Facility
import cityblock.models.gold.PharmacyClaim.{Pharmacy => GoldPharmacy}
import cityblock.models.gold.ProfessionalClaim.Professional
import cityblock.transforms.Transform
import cityblock.transforms.Transform.CodeSet
import cityblock.utilities.CCISilverClaimsMocks._
import cityblock.utilities.Insurance.LineOfBusiness
import cityblock.utilities.Should._
import cityblock.utilities.reference.tables
import cityblock.utilities.reference.tables.{DiagnosisRelatedGroup, PlaceOfService, RevenueCode}
import com.spotify.scio.bigquery._
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.values.SCollection
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat
import org.scalatest.Ignore
import org.scalatest.tagobjects.Slow

@Ignore
class PolishCCIClaimsTest extends PipelineSpec {
  val environmentName = "test"
  val project = "cityblock-data"
  val dataProject = "connecticare-data"
  private val deliveryDate = "20200109"
  private val dtf = DateTimeFormat.forPattern(Transform.ShardNamePattern)

  case class Inputs(
    ProviderDetail: Seq[ProviderDetail],
    ProviderAddress: Seq[ProviderAddress],
    Member: Seq[ConnecticareSilverClaims.Member],
    Medical: Seq[Medical],
    MedicalDiagnosis: Seq[MedicalDiagnosis],
    MedicalICDProcedure: Seq[MedicalICDProcedure],
    LabResults: Seq[LabResults],
    Pharmacy: Seq[Pharmacy],
    Pharmacy_med: Seq[PharmacyMedicare],
    UBH: Seq[UBH],
    UBH_med: Seq[UBHMedicare],
    UBHDiagnosis: Seq[UBHDiagnosis]
  ) {
    def patientId: Option[String] = Member.head.patient.patientId

    def nmi: String = Member.head.patient.externalId

    def union(right: Inputs): Inputs = Inputs(
      ProviderDetail ++ right.ProviderDetail,
      ProviderAddress ++ right.ProviderAddress,
      Member ++ right.Member,
      Medical ++ right.Medical,
      MedicalDiagnosis ++ right.MedicalDiagnosis,
      MedicalICDProcedure ++ right.MedicalICDProcedure,
      LabResults ++ right.LabResults,
      Pharmacy ++ right.Pharmacy,
      Pharmacy_med ++ right.Pharmacy_med,
      UBH ++ right.UBH,
      UBH_med ++ right.UBH_med,
      UBHDiagnosis ++ right.UBHDiagnosis
    )

    def setLineOfBusiness(lob: String, slob: String): Inputs = Inputs(
      ProviderDetail,
      ProviderAddress,
      Member.map(m => m.copy(member = m.member.copy(LOB1 = Some(lob), LOB2 = Some(slob)))),
      Medical.map(m => m.copy(medical = m.medical.copy(LOB1 = Some(lob), LOB2 = Some(slob)))),
      MedicalDiagnosis,
      MedicalICDProcedure,
      LabResults.map(l => l.copy(results = l.results.copy(LOB1 = Some(lob)))),
      Pharmacy.map(p => p.copy(pharmacy = p.pharmacy.copy(LOB1 = Some(lob), LOB2 = Some(slob)))),
      Pharmacy_med.map(p =>
        p.copy(pharmacy = p.pharmacy.copy(LOB1 = Some(lob), LOB2 = Some(slob)))),
      UBH,
      UBH_med,
      UBHDiagnosis
    )

    def addPharmacy(): Inputs = {
      val prescriberDetail = providerDetail()
      val prescriberAddress = providerAddress(prescriberDetail.detail.ProvNum)

      val pharmacyDetail = providerDetail()
      val pharmacyAddress = providerAddress(pharmacyDetail.detail.ProvNum)

      val claim = pharmacy(NMI = nmi,
                           patientId = patientId,
                           PrescribProvNum = Some(prescriberDetail.detail.ProvNum),
                           PrescribPharmacyNum = Some(pharmacyDetail.detail.ProvNum))

      this.copy(
        ProviderDetail = ProviderDetail ++ Seq(pharmacyDetail, prescriberDetail),
        ProviderAddress = ProviderAddress ++ Seq(pharmacyAddress, prescriberAddress),
        Pharmacy = Pharmacy ++ Seq(claim)
      )
    }

    def addPharmacyMedicare(): Inputs = {
      val prescriberDetail = providerDetail()
      val prescriberAddress = providerAddress(prescriberDetail.detail.ProvNum)

      val pharmacyDetail = providerDetail()
      val pharmacyAddress = providerAddress(pharmacyDetail.detail.ProvNum)

      val claim = pharmacyMedicare(NMI = nmi,
                                   patientId = patientId,
                                   PrescribProvNum = Some(prescriberDetail.detail.ProvNum),
                                   PrescribPharmacyNum = Some(pharmacyDetail.detail.ProvNum))

      this.copy(
        ProviderDetail = ProviderDetail ++ Seq(pharmacyDetail, prescriberDetail),
        ProviderAddress = ProviderAddress ++ Seq(pharmacyAddress, prescriberAddress),
        Pharmacy_med = Pharmacy_med ++ Seq(claim)
      )
    }
  }

  def mkTestObjects(patientId: String, NMI: String): Inputs = {
    val uuid = Some(patientId)

    val pcp = providerDetail(ProvNPI = Some("PCP NPI"))
    val referring = providerDetail(ProvNPI = Some("referring NPI"))
    val billing = providerDetail(ProvNPI = Some("billing NPI"))
    val servicing = providerDetail(ProvNPI = Some("servicing NPI"))

    val pcpAddress = providerAddress(pcp.detail.ProvNum)
    val referringAddress = providerAddress(referring.detail.ProvNum)
    val billingAddress = providerAddress(billing.detail.ProvNum)
    val servicingAddress = providerAddress(servicing.detail.ProvNum)

    val member1 =
      member(NMI = NMI, PCP_Num = Some(referring.detail.ProvNum), patientId = uuid)
    val member2 = member(NMI = member1.member.NMI,
                         PCP_Num = Some(pcp.detail.ProvNum),
                         ApplyMo = Some(LocalDate.parse("2019-02-25")),
                         patientId = uuid)

    val professional1 = {
      val tmp = professional(
        NMI = member1.member.NMI,
        ClmServNum = "100",
        ServProvTIN = Some(billing.detail.ProvNum),
        RefProvNum = Some(referring.detail.ProvNum),
        ServProvNum = Some(servicing.detail.ProvNum),
        patientId = uuid
      )
      tmp.copy(medical = tmp.medical.copy(Proc1 = Some("cpt code"), Proc1Type = Some("CPT")))
    }

    val professional2 = {
      val tmp = professional(ClmNum = professional1.medical.ClmNum,
                             ClmServNum = "200",
                             NMI = member1.member.NMI,
                             patientId = uuid)
      tmp.copy(
        medical = tmp.medical.copy(
          Proc1 = None,
          Proc1Type = None,
          Proc2 = Some("hcpcs code"),
          Proc2Type = Some("HCPC")
        ))
    }

    val professionalDiagnoses = Seq(
      diagnosis(professional1.medical.ClmNum, DiagCategory = "Principal"),
      diagnosis(professional1.medical.ClmNum, DiagCategory = "Admitting"),
      diagnosis(professional1.medical.ClmNum, DiagCd = "foo"),
      diagnosis(professional1.medical.ClmNum, DiagCd = "bar"),
      diagnosis(ClmNum = "doesn't match anything", DiagCd = "baz")
    )

    val facility1 =
      facility(
        ClmServNum = "100",
        NMI = member1.member.NMI,
        RefProvNum = Some(referring.detail.ProvNum),
        ServProvNum = Some(servicing.detail.ProvNum),
        patientId = uuid
      )
    val facility2 =
      facility(
        ClmNum = facility1.medical.ClmNum,
        ClmServNum = "99",
        NMI = member1.member.NMI,
        ServProvTIN = Some(billing.detail.ProvNum),
        RefProvNum = Some(referring.detail.ProvNum),
        ServProvNum = Some(servicing.detail.ProvNum),
        patientId = uuid
      )

    val facilityDiagnoses =
      Seq(
        diagnosis(facility1.medical.ClmNum, DiagCategory = "Principal"),
        diagnosis(facility1.medical.ClmNum, DiagCd = "foo"),
        diagnosis(facility1.medical.ClmNum, DiagCd = "bar"),
        diagnosis("doesn't match anything", DiagCd = "baz")
      )

    val facilityProcedures =
      Seq(
        procedure(facility1.medical.ClmNum, ICDProcCd = "fizz"),
        procedure(facility1.medical.ClmNum, ICDProcCd = "buzz"),
        procedure("doesn't match anything", ICDProcCd = "bizz")
      )

    val labResults =
      Seq(
        labResult(
          member1.member.NMI,
          ResultLiteral = Some("NOT DETECTED"),
          CPTCode = Some("87491"),
          NationalResultCode = Some("11268-0"),
          patientId = uuid
        ),
        labResult(
          member1.member.NMI,
          ResultLiteral = Some("0.1"),
          TestName = Some("ABSOLUTE BASO"),
          TestCode = Some("207102"),
          patientId = uuid
        )
      )

    Inputs(
      Seq(pcp, referring, billing, servicing),
      Seq(pcpAddress, referringAddress, billingAddress, servicingAddress),
      Seq(member1, member2),
      Seq(professional1, professional2, facility1, facility2),
      professionalDiagnoses ++ facilityDiagnoses,
      facilityProcedures,
      labResults,
      Seq(),
      Seq(),
      Seq(ubh("MemberId", None)),
      Seq(ubhMedicare("MemberId", Some("BillCode"))),
      Seq(ubhDiagnosis)
    )
  }

  private def in[T](name: String): BigQueryIO[T] = {
    val table = Transform.shardName(name, LocalDate.parse(deliveryDate, dtf))
    BigQueryIO[T](s"SELECT * FROM `$dataProject.silver_claims.$table`")
  }

  private def out[T](name: String): BigQueryIO[T] = {
    val table = Transform.shardName(name, LocalDate.parse(deliveryDate, dtf))
    BigQueryIO[T](s"$dataProject:gold_claims.$table")
  }

  "PolishCCIClaims" should "complete basic transformation" taggedAs Slow in {
    object CheckFacility {
      private def checkDxs(xs: SCollection[Facility]): Unit = {
        satisfyIterable(xs)(_.exists(_.header.diagnoses.nonEmpty))
        satisfyIterable(xs)(_.exists(_.header.diagnoses.exists(_.tier == "principal")))
        satisfyIterable(xs)(_.exists(_.header.diagnoses.exists(_.code == "foo")))
        satisfyIterable(xs)(_.exists(_.header.diagnoses.exists(_.code == "bar")))
        satisfyIterable(xs)(_.exists(!_.header.diagnoses.exists(_.code == "baz")))
        satisfyIterable(xs)(
          _.exists(_.header.diagnoses.forall(_.codeset == CodeSet.ICD10Cm.toString)))
      }

      private def checkProvs(xs: SCollection[Facility]): Unit = {
        xs.flatMap(_.header.provider.billing) shouldNot beEmpty
        xs.flatMap(_.header.provider.referring) shouldNot beEmpty
        xs.flatMap(_.header.provider.servicing) shouldNot beEmpty
        xs.flatMap(_.header.provider.operating) should beEmpty
      }

      private def checkPxs(xs: SCollection[Facility]): Unit = {
        satisfyIterable(xs)(_.exists(_.header.procedures.nonEmpty))
        satisfyIterable(xs)(_.exists(!_.header.procedures.exists(_.tier == "principal")))
        satisfyIterable(xs)(_.exists(_.header.procedures.exists(_.tier == "secondary")))
        satisfyIterable(xs)(_.exists(_.header.procedures.exists(_.code == "fizz")))
        satisfyIterable(xs)(_.exists(_.header.procedures.exists(_.code == "buzz")))
        satisfyIterable(xs)(_.exists(!_.header.procedures.exists(_.code == "bizz")))
        xs.flatMap(_.header.procedures)
          .map(_.codeset)
          .distinct should containInAnyOrder(List(CodeSet.ICD10Pcs.toString))
      }

      def checkAll(xs: SCollection[Facility], patientId: Option[String]): Unit = {
        satisfyIterable(xs)(_.exists(_.memberIdentifier.patientId == patientId))

        xs.map(_.header.typeOfBill).distinct should containValue(Option("facility.BillTypeCd"))

        checkDxs(xs)
        checkProvs(xs)
        checkPxs(xs)
      }
    }

    object CheckProfessional {
      private def checkDxs(xs: SCollection[Professional]): Unit = {
        satisfyIterable(xs)(_.exists(_.header.diagnoses.nonEmpty))
        satisfyIterable(xs)(_.exists(_.header.diagnoses.exists(_.tier == "principal")))
        satisfyIterable(xs)(_.exists(!_.header.diagnoses.exists(_.tier == "admit")))
        satisfyIterable(xs)(_.exists(_.header.diagnoses.exists(_.code == "foo")))
        satisfyIterable(xs)(_.exists(_.header.diagnoses.exists(_.code == "bar")))
        satisfyIterable(xs)(_.exists(!_.header.diagnoses.exists(_.code == "baz")))
      }

      def checkAll(xs: SCollection[Professional], patientId: Option[String]): Unit = {
        satisfyIterable(xs)(_.exists(_.memberIdentifier.patientId == patientId))
        satisfyIterable(xs)(_.exists(_.lines.exists(_.provider.servicing.nonEmpty)))
        satisfyIterable(xs)(_.exists(_.lines.exists(_.provider.servicing.isEmpty)))
        xs.flatMap(_.lines)
          .flatMap(_.procedure)
          .map(_.codeset) should containInAnyOrder(
          Seq(CodeSet.CPT.toString, CodeSet.HCPCS.toString))
        satisfyIterable(xs)(_.exists(_.header.provider.referring.nonEmpty))
        satisfyIterable(xs)(_.exists(_.header.provider.billing.nonEmpty))
        checkDxs(xs)
      }
    }

    def checkMember(member: Claims.Member): Boolean =
      member.attributions
        .maxBy(_.date.from)(Transform.NoneMinOptionLocalDateOrdering)
        .PCPId
        .nonEmpty

    def checkLabResults(xs: SCollection[LabResult]): Unit = {
      xs should haveSize(4)
      xs.flatMap(_.procedure).map(_.code).distinct should
        containSingleValue("87491")
      xs.flatMap(_.procedure).map(_.codeset).distinct should
        containSingleValue(CodeSet.CPT.toString)
      xs.flatMap(_.name).distinct should containSingleValue("ABSOLUTE BASO")
      xs.flatMap(_.result).distinct should containInAnyOrder(Seq("0.1", "NOT DETECTED"))
      xs.flatMap(_.loinc).distinct should
        containInAnyOrder(Seq("207102", "11268-0"))
    }

    def checkPharmacy(xs: SCollection[GoldPharmacy], inputs: Inputs): Unit = {
      xs should haveSize(2)
      xs.flatMap(_.prescriber) should haveSize(2)
      xs.flatMap(_.pharmacy) should haveSize(2)
    }

    val commercial =
      mkTestObjects("commercial patientId", "commercial NMI")
        .addPharmacy()
        .setLineOfBusiness("C", "EX")
    val medicare =
      mkTestObjects("medicare patientId", "medicare NMI")
        .addPharmacyMedicare()
        .setLineOfBusiness("M", "M")

    val inputs = commercial.union(medicare)

    JobTest[PolishCCIClaims.type]
      .args(
        s"--project=$project",
        s"--environment=$environmentName",
        s"--dataProject=$dataProject",
        s"--destinationProject=$dataProject",
        s"--deliveryDate=$deliveryDate"
      )
      .input(in[ProviderDetail]("ProviderDetail"), commercial.ProviderDetail)
      .input(in[ProviderDetail]("ProviderDetail_med"), medicare.ProviderDetail)
      .input(in[ProviderAddress]("ProviderAddresses"), commercial.ProviderAddress)
      .input(in[ProviderAddress]("ProviderAddresses_med"), medicare.ProviderAddress)
      .input(in[ConnecticareSilverClaims.Member]("Member"), commercial.Member)
      .input(in[ConnecticareSilverClaims.Member]("Member_med"), medicare.Member)
      .input(in[Medical]("Medical"), commercial.Medical)
      .input(in[Medical]("Medical_med"), medicare.Medical)
      .input(in[MedicalDiagnosis]("MedicalDiagnosis"), commercial.MedicalDiagnosis)
      .input(in[MedicalDiagnosis]("MedicalDiagnosis_med"), medicare.MedicalDiagnosis)
      .input(in[MedicalICDProcedure]("MedicalICDProc"), commercial.MedicalICDProcedure)
      .input(in[MedicalICDProcedure]("MedicalICDProc_med"), medicare.MedicalICDProcedure)
      .input(in[Pharmacy]("Pharmacy"), inputs.Pharmacy)
      .input(in[PharmacyMedicare]("Pharmacy_med"), inputs.Pharmacy_med)
      .input(in[LabResults]("LabResults"), commercial.LabResults ++ medicare.LabResults)
      .input(BigQueryIO(DiagnosisRelatedGroup.queryDistinct),
             Seq(DiagnosisRelatedGroup.Valid("0024"), DiagnosisRelatedGroup.Valid("0023")))
      .input(BigQueryIO(RevenueCode.queryDistinct), Seq())
      .input(BigQueryIO(PlaceOfService.queryDistinct), Seq())
      .input(BigQueryIO(tables.TypeOfBill.queryDistinct), Seq())
      .output(out[Provider]("Provider")) { xs =>
        xs should haveSize(inputs.ProviderDetail.length)
      }
      .output(out[Claims.Member]("Member")) { xs =>
        xs.filter(_.eligibilities.head.detail.lineOfBusiness.contains(
          LineOfBusiness.Commercial.toString)) should
          satisfySingleValue[Claims.Member](checkMember)
        xs.filter(_.eligibilities.head.detail.lineOfBusiness.contains(
          LineOfBusiness.Medicare.toString)) should
          satisfySingleValue[Claims.Member](checkMember)
      }
      .output(out[Professional]("Professional")) { xs =>
        val c = xs.filter(_.header.lineOfBusiness.contains(LineOfBusiness.Commercial.toString))
        val m = xs.filter(_.header.lineOfBusiness.contains(LineOfBusiness.Medicare.toString))

        CheckProfessional.checkAll(c, commercial.Member.head.patient.patientId)
        CheckProfessional.checkAll(m, medicare.Member.head.patient.patientId)
      }
      .output(out[Facility]("Facility")) { xs =>
        val c = xs.filter(_.header.lineOfBusiness.contains(LineOfBusiness.Commercial.toString))
        val m =
          xs.filter(_.header.lineOfBusiness.contains(LineOfBusiness.Medicare.toString))

        CheckFacility.checkAll(c, commercial.Member.head.patient.patientId)
        CheckFacility.checkAll(m, medicare.Member.head.patient.patientId)
      }
      .output(out[GoldPharmacy]("Pharmacy")) { xs =>
        checkPharmacy(xs, inputs)
      }
      .output(out[LabResult]("LabResult")) { xs =>
        checkLabResults(xs)
      }
      .run()
  }
}
