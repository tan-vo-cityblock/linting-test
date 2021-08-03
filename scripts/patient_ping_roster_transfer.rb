#! /usr/bin/env ruby
#
# This script loads Patient Ping roster information from BigQuery
# and transfers it to their SFTP site
require_relative "environment"
require "google/cloud/bigquery"
require "csv"
require "net/sftp"

QUERY = File.read(File.join(File.dirname(__FILE__), 'patient_ping_roster.sql'))

GCP_PROJECT = "cbh-db-mirror-prod"
FILE_PATH = ENV["FILE_PATH"] || "/tmp/patient_ping_roster.csv"

bigquery = Google::Cloud::Bigquery.new project_id: GCP_PROJECT

results = bigquery.query QUERY

begin
  if results.length > 0
    CSV.open(FILE_PATH, "wb", col_sep: ",", headers: true) do |csv|
      # Actually add headers
      csv << results.first.keys.map { |k| k.to_s }

      results.each do |row|
        csv << row.values
      end
    end

    Net::SFTP.start('put.patientping.com', 'cityblock', password: ENV["PATIENT_PING_PASSWORD"], port: '22999', verify_host_key: :always) do |sftp|
      sftp.upload!(FILE_PATH, "/in/cityblock_roster.csv")
    end
  end
ensure
  File.delete(FILE_PATH)
end
