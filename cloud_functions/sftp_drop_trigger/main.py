from cf.define import *
from cityblock.partner.conditions import partner
import google.cloud.logging

client = google.cloud.logging.Client()

client.get_default_handler()
client.setup_logging()


cf = fn.on_gcs_blob(
    (ignore, ext(".pgp")),  # TODO: get decryption function working
    *partner.healthyblue(
        (
            "healthyblue_professional_v1",
            ["NCMT_MEDENCCLMPHD_BCBS_CB_%s", "NCMT_MEDENCCLMPLN_BCBS_CB_%s"],
            "TXT",
        ),
        (
            "healthyblue_institutional_v1",
            ["NCMT_MEDENCCLMIHD_BCBS_CB_%s", "NCMT_MEDENCCLMILN_BCBS_CB_%s"],
            "TXT",
        ),
        (
            "healthyblue_pharmacy_v1",
            ["NCMT_RXENCHD_BCBS_CB_%s", "NCMT_RXENCLN_BCBS_CB_%s"],
            "TXT",
        ),
        (
            "healthyblue_member_v1",
            ["NCMT_BeneficiaryAssignmentData_BCBS_CB_%s"],
            "TXT",
        ),
        ("healthyblue_provider_v1", ["CB_NPI_Data_%s"], "TXT"),
        (
            "healthyblue_risk_v1",
            ["NCMT_CareQualityManagement_AMH_PatientListRiskScore_BCBS_CB_%s"],
            "TXT",
        ),
    )
)
