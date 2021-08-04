#!/usr/bin/env ruby
require_relative 'environment'
require 'google/cloud/bigquery'
require 'google/cloud/datastore'
require 'pry'

DATASTORE = Google::Cloud::Datastore.new project_id: 'cityblock-data'

def bigquery_data
  bigquery = Google::Cloud::Bigquery.new project_id: 'cityblock-data'
  bigquery.query %Q[
  SELECT
    patientId,
    CASE
      WHEN calculated IS NOT NULL THEN calculated
      ELSE (CASE
        WHEN category = "High Risk / BH" THEN "High Risk"
        ELSE category END)
    END AS category
  FROM
    `cityblock-data.manual_load_datasets.cohort_assignment`
  ]
end

def datastore_data
  query = DATASTORE.query('Patient').where('cohort', '>', 0)
  page = DATASTORE.run query
  all = []
  loop do
    puts page.length
    all << page
    break if !(page = page.next)
  end
  all.flatten
end

patient_categories = bigquery_data
entries = datastore_data

puts "#{patient_categories.length} patient_categories"
puts "#{entries.length} entries"

updates = entries.map do |entry|
  patient_category = patient_categories.find do |patient_category|
    entry[:id] == patient_category[:patientId]
  end
  next if !patient_category || entry[:category]
  entry[:category] = patient_category[:category]
  entry
end

updates.compact.each { |update| DATASTORE.save update }
