package cityblock.member.service.utilities

case class MultiPartnerIds(
  market: String,
  partner: String,
  clinic: String
)

object MultiPartnerIds {
  private val fake = MultiPartnerIds(
    market = "fakemarket",
    partner = "fakepartner",
    clinic = "fakeclinic"
  )

  private val EmblemProduction = MultiPartnerIds(
    market = "3d8e4e1d-bdb5-4676-9e25-9b6fa248101f",
    partner = "5749b36b-8b04-4323-9a7d-fc99b242bf7d",
    clinic = "b250a071-b261-4af7-90e2-67d2e22789ac"
  )

  private val EmblemStaging = MultiPartnerIds(
    market = "1bf5a200-cdb9-414d-9778-a584cd5bef8c",
    partner = "97869ccd-4d36-4665-b827-9b73e5db5f22",
    clinic = "b250a071-b261-4af7-90e2-67d2e22789ac"
  )

  private val EmblemVirtualProduction = MultiPartnerIds(
    market = "f765efc5-04a4-494a-aecc-dc150bd2907d",
    partner = "5749b36b-8b04-4323-9a7d-fc99b242bf7d",
    clinic = "9b08421a-540b-4c83-b25e-73a234c6ad45"
  )

  private val EmblemVirtualStaging = MultiPartnerIds(
    market = "cb3d3c98-964a-47a2-a235-11d20b910d1c",
    partner = "97869ccd-4d36-4665-b827-9b73e5db5f22",
    clinic = "fda4ebe7-c1bc-4ebb-9af3-2b8d23d3f4c0"
  )

  private val ConnecticareProduction = MultiPartnerIds(
    market = "9065e1fc-2659-4828-9519-3ad49abf5126",
    partner = "7730391e-402f-43dc-97b7-68fab14d0e0f",
    clinic = "77ea58cf-3a0e-4df8-9397-92ed1b923e1d"
  )

  private val ConnecticareStaging = MultiPartnerIds(
    market = "84e127f6-7c3a-4893-9921-3c52c3b8a1d8",
    partner = "3f63e75e-6b1d-4abc-b102-f6c2f7a13de6",
    clinic = "b250a071-b261-4af7-90e2-67d2e22789ac"
  )

  private val TuftsVirtualProduction = MultiPartnerIds(
    market = "6a4df00d-6570-4f03-9d29-de01cc22b128",
    partner = "88bfbd37-b64c-4d12-82ac-8efa68da8c45",
    clinic = "d8e23d0d-ec45-4971-ac4a-4192db31720d"
  )

  private val TuftsProduction = MultiPartnerIds(
    market = "76150da6-b62a-4099-9bd6-42e7be3ffc62",
    partner = "88bfbd37-b64c-4d12-82ac-8efa68da8c45",
    clinic = "d8e23d0d-ec45-4971-ac4a-4192db31720d"
  )

  private val TuftsStaging = MultiPartnerIds(
    market = "6b7a43ac-7a2a-4ee5-97cf-480ddf8abf09",
    partner = "fe29ac86-dd6c-496d-8096-956b0d3e61a9",
    clinic = "ca330b73-8d09-4f36-9e81-4e40a825b696"
  )

  private val CareFirstProduction = MultiPartnerIds(
    market = "31e02957-0bac-475b-b306-a6497a3cb823",
    partner = "6480bf53-5a4d-47e3-a656-8ab66a37dde8",
    clinic = "617852ed-4169-4886-8d9c-37c5cfb7732f"
  )

  private val CareFirstStaging = MultiPartnerIds(
    market = "ae050526-a951-4d07-b0ff-bd1d7239ac76",
    partner = "46ccb90a-b817-4b0f-a70a-94250837db10",
    clinic = "ba100b13-f1ae-4e64-90fb-f5ef2050b7f6"
  )

  private val CardinalStaging = MultiPartnerIds(
    market = "dynamic",
    partner = "46ccb90a-b817-4b0f-a70a-94250837db11",
    clinic = "dynamic"
  )

  private val CardinalProduction = MultiPartnerIds(
    market = "31c505ee-1e1b-4f5c-8e3e-4a2bc9937e04",
    partner = "096f5142-427c-4534-9b3e-a6ad6ff4ab61",
    clinic = "dynamic"
  )

  private val HealthyBlueStaging = MultiPartnerIds(
    market = "dynamic",
    partner = "ed2a1e3f-072e-4645-817c-c8112ff35745",
    clinic = "dynamic"
  )

  private val HealthyBlueProduction = MultiPartnerIds(
    market = "31c505ee-1e1b-4f5c-8e3e-4a2bc9937e04",
    partner = "ed2a1e3f-072e-4645-817c-c8112ff35745",
    clinic = "dynamic"
  )

  def getByProject(project: String): MultiPartnerIds = project.toLowerCase match {
    case "emblemproduction"        => EmblemProduction
    case "emblemstaging"           => EmblemStaging
    case "connecticareproduction"  => ConnecticareProduction
    case "connecticarestaging"     => ConnecticareStaging
    case "tuftsproduction"         => TuftsProduction
    case "tuftsstaging"            => TuftsStaging
    case "tuftsvirtualproduction"  => TuftsVirtualProduction
    case "emblemvirtualproduction" => EmblemVirtualProduction
    case "emblemvirtualstaging"    => EmblemVirtualStaging
    case "carefirstproduction"     => CareFirstProduction
    case "carefirststaging"        => CareFirstStaging
    case "cardinalstaging"         => CardinalStaging
    case "cardinalproduction"      => CardinalProduction
    case "healthybluestaging"      => HealthyBlueStaging
    case "healthyblueproduction"   => HealthyBlueProduction
    case _                         => throw new Exception("Requested Partner does not exist")
  }
}
