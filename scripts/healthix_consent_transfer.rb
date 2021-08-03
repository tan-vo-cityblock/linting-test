#! /usr/bin/env ruby
#
# This script loads Healthix consent information from BigQuery
# and transfers it to their SFTP site
require_relative "environment"
require "google/cloud/bigquery"
require "csv"
require "net/sftp"

QUERY = File.read(File.join(File.dirname(__FILE__), 'healthix_consent.sql'))

GCP_PROJECT = "cbh-db-mirror-prod"
FILE_PATH = ENV["FILE_PATH"] || "/tmp/healthix.csv"

bigquery = Google::Cloud::Bigquery.new project_id: GCP_PROJECT

results = bigquery.query QUERY

CSV.open(FILE_PATH, "wb", col_sep: "|", headers: false) do |csv|
  results.each do |row|
    csv << row.values
  end
end

Net::SFTP.start('sftp.healthix.org', 'CITYBLOCK_SFTPUser', password: ENV["HEALTHIX_PASSWORD"], verify_host_key: :always) do |sftp|
  sftp.upload!(FILE_PATH, "/in/healthix.csv")
end

File.delete(FILE_PATH)
