import csv
import logging
import re
from xlrd import open_workbook, xldate_as_tuple
from ntpath import basename, split, splitext
from datetime import datetime, date

from preprocess_payer_data.download_payer_file import (
    EMBLEM_SFTP_SUBDIR,
    CONNECTICARE_SFTP_SUBDIR,
)

ADDL_FIELDS = ["riskAdjustmentModel", "dateUploaded"]
LOWER_FIELD_MAPPINGS = {
    "nmi (dec)": "nmiOld",
    "nmi (last)": "nmi",
    "nmi": "nmi",
    "member id": "nmi",
    "cohort": "cohort",
    "segment": "segment",
    "lob": "lob",
    "line of business": "lob",
    "current category": "acuity",
    "member name": "name",
    "acuity": "acuity",
    "mbr first name": "firstName",
    "member first name": "firstName",
    "mbr last name": "lastName",
    "member last name": "lastName",
    "dob": "dateOfBirth",
    "date of birth": "dateOfBirth",
    "gender": "gender",
    "conditiontype": "conditionType",
    "confidencelevel": "confidenceLevel",
    "gov risk factor": "riskScoreImpact",
    "govriskfactor": "riskScoreImpact",
    "hhs risk score": "riskScoreImpact",
    "risk score impact": "riskScoreImpact",
    "hcc": "conditionCategoryCode",
    "condition": "conditionCategoryCode",
    "hcc description": "conditionCategoryDescription",
    "condition desc": "conditionCategoryDescription",
    "diagnosis code": "underlyingDxCode",
    "diagnosis code description": "underlyingDxCodeDescription",
    "underlying_cd": "underlyingDxCode",
    "last coded": "underlyingDxLastCodedDate",
    "provider": "underlyingDxLastCodedProvider",
    "npi": "underlyingDxLastCodedProviderNpi",
}
BACKFILL_RISK_MODEL_MAPPINGS = {
    "mr": "HCC",
    "md": "3M CRG",
    "commercial": "HHS-HCC",
    "medicare": "HCC",
}
HEADER_ROW = 0
DOB_COL = 4
PROCESSED_FILE_INFO = {}
LOGGER = logging.getLogger("airflow.task")


class TransformFieldsDataFile:
    def __init__(self, lcl_file_paths: [str]):
        self.xlsx_f = filter(
            lambda file_path: file_path.endswith(".xlsx"), lcl_file_paths
        )
        self.csv_f = filter(
            lambda file_path: file_path.endswith(".csv") or file_path.endswith(".txt"),
            lcl_file_paths,
        )

    def process_files_record_meta(self, **context):
        for xlsx_filepath in self.xlsx_f:
            self.__process_xlsx(xlsx_filepath)
        for csv_filepath in self.csv_f:
            self.__process_csv(csv_filepath)
        for provider_type, value in PROCESSED_FILE_INFO.items():
            context["ti"].xcom_push(key=provider_type, value=value)

    def __process_csv(self, filepath: str):
        with open(filepath, "r") as csv_file:
            csv_reader = csv.reader(csv_file)
            csv_data = [row for row in csv_reader]
        LOGGER.info(f"ORIGINAL HEADER ROW >>> {csv_data[HEADER_ROW]}")
        processed_header = [
            LOWER_FIELD_MAPPINGS.get(cell.strip().lower(), cell)
            for cell in csv_data[HEADER_ROW]
        ]
        csv_data[HEADER_ROW] = processed_header
        LOGGER.info(f"PROCESSED HEADER ROW >>> {csv_data[HEADER_ROW]}")
        new_file_abs_path = self.__get_file_path(filepath)
        LOGGER.info(f"NEW FILE PATH >>> {new_file_abs_path}")
        appended_file_data = self.__backfill_file_cols(filepath, csv_data)
        self.__write_csv(new_file_abs_path, appended_file_data)

    def __process_xlsx(self, filepath: str):
        workbook = open_workbook(filepath)
        sheet_names = workbook.sheet_names()
        for name in sheet_names:
            file_data = []
            worksheet = workbook.sheet_by_name(name)
            num_rows, num_cols = worksheet.nrows, worksheet.ncols
            header = worksheet.row(HEADER_ROW)
            LOGGER.info(f"ORIGINAL HEADER ROW >>> {header}")
            file_data.append(
                [
                    LOWER_FIELD_MAPPINGS.get(cell.value.strip().lower(), cell.value)
                    for cell in header
                ]
            )
            LOGGER.info(f"PROCESSED HEADER ROW >>> {file_data[HEADER_ROW]}")
            for curr_row in range(1, num_rows):
                file_data.append(
                    [
                        date.strftime(
                            datetime(
                                *xldate_as_tuple(
                                    worksheet.cell(curr_row, curr_col).value,
                                    workbook.datemode,
                                )
                            ),
                            "%Y-%m-%d",
                        )
                        if (
                            worksheet.cell(curr_row, curr_col).value
                            and curr_col == DOB_COL
                        )
                        else worksheet.cell(curr_row, curr_col).value
                        for curr_col in range(num_cols)
                    ]
                )
            new_file_abs_path = self.__get_file_path(filepath, name)
            LOGGER.info(f"NEW FILE PATH >>> {new_file_abs_path}")
            appended_file_data = self.__backfill_file_cols(new_file_abs_path, file_data)
            self.__write_csv(new_file_abs_path, appended_file_data)

    def __backfill_file_cols(self, filepath: str, file_data: [[str]]):
        risk_model_or = r"|"
        risk_model_file_regex = r".*?(?: |_)(?P<risk_model_key>{})(?: |_)".format(
            risk_model_or.join(BACKFILL_RISK_MODEL_MAPPINGS.keys())
        )
        risk_model_filename_pattern = re.match(
            risk_model_file_regex, basename(filepath), re.IGNORECASE
        )
        risk_model = BACKFILL_RISK_MODEL_MAPPINGS[
            risk_model_filename_pattern["risk_model_key"].lower()
        ]
        timestamp = str(self.__get_date_from_file_name(filepath))
        file_data[HEADER_ROW].extend(ADDL_FIELDS)
        for row_idx in range(1, len(file_data)):
            file_data[row_idx].extend([risk_model, timestamp])
        return file_data

    def __write_csv(self, filepath: str, content: [[str]]):
        with open(filepath, "w") as csv_file:
            csv_writer = csv.writer(csv_file)
            for row in content:
                csv_writer.writerow(row)
        self.__store_partner_file_info(filepath)

    def __get_file_path(self, filepath: str, sheet_name: str = ""):
        file_split_path = split(filepath)
        LOGGER.info(f"FILE SPLIT PATH >>> {file_split_path}")
        file_dir = file_split_path[0]
        file_name = file_split_path[1]
        full_date = self.__get_date_from_file_name(file_name)
        new_file_name = re.sub(r"[0-9]{4}-?[0-9]{2}-?[0-9]{2}", full_date, file_name)
        new_file_name_strp_cohort = re.sub(
            r" [Cc]ohort [0-9]-[0-9]{1,2}[a-z] ", " ", new_file_name
        )
        sheet_name = "_" + sheet_name if sheet_name else sheet_name
        final_full_path = f"{file_dir}/{splitext(new_file_name_strp_cohort)[0]}{sheet_name}_PROCESSED.csv"
        LOGGER.info(f"FINAL FULL PATH >>> {final_full_path}")
        return final_full_path

    def __store_partner_file_info(self, final_full_path: str):
        partner_name = self.__get_partner_name(final_full_path)
        full_date = self.__get_date_from_file_name(final_full_path)
        is_med_file = (
            ("_medicare_" in final_full_path.lower())
            or ("_mr_" in final_full_path.lower())
            or (" mr " in final_full_path.lower())
        )
        partner_key = partner_name + "_med" if is_med_file else partner_name
        PROCESSED_FILE_INFO[partner_key] = full_date

    @staticmethod
    def __get_partner_name(filename: str):
        partner_name_pattern = re.match(
            r".*/(?P<partner_name>{}|{})/.*".format(
                CONNECTICARE_SFTP_SUBDIR, EMBLEM_SFTP_SUBDIR
            ),
            filename,
        )
        return partner_name_pattern.group("partner_name")

    @staticmethod
    def __get_date_from_file_name(filename: str):
        date_match_file_pattern = re.match(
            r".*?(?:(?P<dash_date>[0-9]{4}-[0-9]{2})|(?P<no_dash_date>[0-9]{6})|(?P<full_date>[0-9]{8}))[._].*",
            filename,
        )
        date_matches = date_match_file_pattern.groupdict()
        if date_matches.get("dash_date"):
            formatted_date = datetime.strptime(date_matches.get("dash_date"), "%Y-%m")
        elif date_matches.get("no_dash_date"):
            formatted_date = datetime.strptime(date_matches.get("no_dash_date"), "%Y%m")
        elif date_matches.get("full_date"):
            formatted_date = datetime.strptime(date_matches.get("full_date"), "%Y%m%d")
        else:
            raise Exception("date regex not found for {}", filename)
        return formatted_date.strftime("%Y%m%d")


def main(**context):
    return TransformFieldsDataFile(
        context["ti"].xcom_pull(task_ids="download_payer_suspect_data_from_gcs")
    ).process_files_record_meta(**context)
