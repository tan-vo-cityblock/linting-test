import argparse
import logging

from google.cloud import storage


class SplitFacetsMembers:
    def __init__(self, write_bucket: str, date: str):
        self.date = date
        self.client = storage.Client(project="cityblock-data")
        self.read_bucket = self.client.get_bucket("cbh_sftp_drop")
        self.write_bucket = self.client.get_bucket(write_bucket)
        self.prefix = f"connecticare_production/drop/CCI_CITYBLOCK_MULTI_MEMELIG_FA_FW01_PROD_{date}"

    def _write_file(self, file_name: str, data: list) -> None:
        write_prefix = "connecticare_facets/split_facets_files"
        destination_blob = self.write_bucket.blob(f"{write_prefix}/{file_name}_{self.date}.txt")
        data_to_upload = "\n".join(data)
        destination_blob.upload_from_string(data_to_upload)

    def run(self) -> None:
        sftp_blobs = self.read_bucket.list_blobs(prefix=self.prefix)

        blobs = list(sftp_blobs)
        if len(blobs) < 1:
            raise IndexError(f"File with prefix {self.prefix} does not exist.")

        members = []
        benefits = []
        pcps = []

        """ 
        TODO while file sizes are small (<100 MB) they can easily fit into memory. When scaling this up we want to 
        output the contents into something like BQ and then have another process do a filter type query on it and do the
        split file logic
        """
        member_file = blobs[0].download_as_string().splitlines()
        for line in member_file:
            line = line.decode("utf-8", errors="ignore")

            if line.startswith("0210"):
                members.append(line)
            elif line.startswith("0230"):
                benefits.append(line)
            elif line.startswith("0240"):
                pcps.append(line)

        self._write_file("FacetsMember_med", members)
        self._write_file("HealthCoverageBenefits_med", benefits)
        self._write_file("AttributedPCP_med", pcps)


def argument_parser():
    cli = argparse.ArgumentParser(description="Argument parser for splitting up CCI Facets file")
    cli.add_argument("--bucket", type=str, required=True, help="GCS Bucket to write data into")
    cli.add_argument("--date", type=str, required=True, help="Date which the files were delivered at")
    return cli


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    # Parse input arguments
    args = argument_parser().parse_args()

    date = args.date
    bucket = args.bucket

    sfm = SplitFacetsMembers(bucket, date)
    sfm.run()

    logging.info("Completed splitting of Facets file")
