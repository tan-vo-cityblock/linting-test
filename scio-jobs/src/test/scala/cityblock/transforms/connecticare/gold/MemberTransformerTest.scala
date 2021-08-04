package cityblock.transforms.connecticare.gold

import cityblock.member.service.models.PatientInfo.{Datasource, Patient}
import cityblock.models.gold.Claims.MemberIdentifier
import cityblock.transforms.connecticare.gold.MemberTransformer.MemberKey
import cityblock.utilities.CCISilverClaimsMocks._
import cityblock.utilities.PartnerConfiguration
import com.spotify.scio.testing.PipelineSpec
import org.joda.time.LocalDate

class MemberTransformerTest extends PipelineSpec {
  "Members with the same patient id but different partner ids" should "be grouped together by MemberKey" in {
    val source = Datasource(name = PartnerConfiguration.connecticare.partner.name, commonId = None)

    val john =
      Patient(patientId = None, externalId = "123", source = source)
    val kate1 =
      Patient(patientId = Some("A"), externalId = "234", source = source)
    val kate2 =
      Patient(patientId = Some("A"), externalId = "456", source = source)

    runWithContext { sc =>
      val grouped =
        sc.parallelize(Seq(john, kate1, kate2)).groupBy(MemberKey(_))

      val johns = grouped.filter(_._1 == MemberKey(Left("123"))).flatMap(_._2)
      val kates = grouped.filter(_._1 == MemberKey(Right("A"))).flatMap(_._2)

      johns should containSingleValue(john)
      kates should containInAnyOrder(Seq(kate1, kate2))
    }
  }

  they should "use the newer NMI as the partnerId when transformed to gold" in {
    val oldMember = member(
      "old NMI",
      ApplyMo = Some(LocalDate.parse("2019-01-01")),
      patientId = Some("patientId")
    )
    val newMember = member(
      "new NMI",
      ApplyMo = oldMember.member.ApplyMo.map(_.plusMonths(1)),
      patientId = oldMember.patient.patientId
    )

    runWithContext { sc =>
      val members =
        MemberTransformer(sc.parallelize(Seq(oldMember, newMember)))
          .transform()("testProject")

      val expected = MemberIdentifier(
        commonId = None,
        partnerMemberId = "new NMI",
        patientId = Some("patientId"),
        partner = newMember.patient.source.name
      )

      members.map(_.identifier) should containSingleValue(expected)
    }
  }
}
