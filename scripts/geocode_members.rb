#! /usr/bin/env ruby
#
# This script loads member addresses from BigQuery
# geocodes them with Geocodio, then stores the results in BigQuery.
require_relative "environment"
require "google/cloud/bigquery"
require 'geocoder'
require 'pry'
require_relative 'lib/hipaa_geocodio'
require 'optparse'

options = {}
OptionParser.new do |opts|
  opts.banner = "Usage: geocode_members.rb [options]"

  opts.on("-k", "--key KEY", "Geocodio API Key") do |v|
    options[:key] = v
  end

  opts.on("-d", "--destination DESTINATION", "BigQuery Destination (e.g. \"geocoding.member_locations\"") do |d|
    raise "Invalid destination #{d}" unless d.match(/(\S+)\.(\S+)/)
    options[:destination] = d
  end

  opts.on("-s", "--source SOURCE", "BigQuery Source (e.g. \"gold_claims.Member\"") do |s|
    raise "Invalid source #{s}" unless s.match(/(\S+)\.(\S+)/)
    options[:source] = s
  end

  opts.on("-p", "--project PROJECT", "Partner project for storing data") do |p|
    options[:project] = p
  end

  opts.on("-h", "--help", "Prints this help") do
    puts opts
    exit
  end
end.parse!
p options

raise "Options not specified, check -h for help" unless options[:key] && options[:destination] && options[:project]

def geocode_members(options)
  dataset_name, table_name = options[:destination].match(/(\S+)\.(\S+)/).captures.map(&:strip)

  Geocoder.configure(geocodio: {api_key: options[:key]}, lookup: :geocodio, kernel_logger_level: ::Logger::DEBUG, use_https: true)
  
  
  
  canary = Geocoder.search("23349 Piney Creek Dr Athens AL 35613")
  gcp_project = options[:project]
  bigquery = Google::Cloud::Bigquery.new project_id: gcp_project
  
  bigquery.create_dataset dataset_name unless bigquery.dataset dataset_name
  dataset = bigquery.dataset dataset_name
  
  def create_member_locations(dataset, table)
    dataset.create_table table do |t|
      t.schema.string "geocodio_results", mode: :required, description: "Raw data returned from the Geocoder from Geocodio"
      t.schema.string "address", mode: :required, description: "Address that was geocoded"
      t.schema.float "latitude", mode: :required, description: "Latitude of member address"
      t.schema.float "longitude", mode: :required, description: "Longitude of member address"
    end
  end
  
  create_member_locations(dataset, table_name) unless dataset.table table_name
  
  query = %Q[
    SELECT
      DISTINCT UPPER(CONCAT(demographics.location.address1, " ", demographics.location.city, " ", demographics.location.state, " ",demographics.location.zip)) AS address
    FROM
      `#{options[:project]}.#{options[:source]}`
    WHERE
      demographics.location.address1 IS NOT NULL
      AND demographics.location.zip IS NOT NULL
    EXCEPT DISTINCT
    SELECT address FROM `#{options[:project]}.#{dataset_name}.#{table_name}`
  ]
  
  results = bigquery.query query, max: 500
  
  # Note, this does use the streaming API which is not free.
  # I'm making the decision that my time is worth more to the company
  # than the cost of the streaming inserts. If we end up inserting millions
  # of records, this should be rewritten to dump the results to a file
  # then loading all at once to BQ. That requires some extra machinery though
  # and this is the simplest way to accomplish the task.
  
  inserter = dataset.insert_async table_name do |result|
    if result.error?
      log_error result.error
    else
      log_insert "inserted #{result.insert_count} rows " \
        "with #{result.error_count} errors"
    end
  end
  
  results.all do |result|
    g_results = Geocoder.search(result[:address])
    if g_results && g_results[0]
      inserter.insert [{ geocodio_results: g_results.map(&:data).to_json, address: result[:address], latitude: g_results[0].location["lat"], longitude: g_results[0].location["lng"] }]
    end
  end
  inserter.stop.wait!
end

geocode_members(options)