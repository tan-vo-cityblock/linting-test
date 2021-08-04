#!/usr/bin/env ruby
#
# This script fetches a CCD for each ACPNY patient in the patient
# index and outputs a JSON file containing all patients whose data of
# birth doesn't match that of the Emblem claims data.

require 'pry'

require_relative 'lib/redox_client'

CONCURRENCY = 40
ENVIRONMENT_VARIABLES = [
  'REDOX_DESTINATION_NAME',
  'REDOX_DESTINATION_ID',
  'REDOX_API_KEY',
  'REDOX_CLIENT_SECRET',
  'CITYBLOCK_DATA_PROJECT_ID',
]

def check_environment_variables
  undefined = ENVIRONMENT_VARIABLES.map { |name| [name, ENV[name]] }
    .to_h
    .select { |k, v| !v || v.empty? }

  if undefined.length > 0
    puts 'Please define: ' + undefined.map { |k, v| k }.join(', ')
    exit 1
  end
end

def fetch_patient_index(project_id:)
  require 'google/cloud/datastore'
  datastore = Google::Cloud::Datastore.new(project_id: project_id)
  query = Google::Cloud::Datastore::Query.new
  query.kind 'Patient'
  page = datastore.run(query)
  index = []
  loop {
    index << page
    break unless page.next?
    page = page.next
  }
  index.flatten
end

def fetch_emblem_demographics(patients:, project_id:)
  require 'google/cloud/bigquery'
  bigquery = Google::Cloud::Bigquery.new(project_id: project_id)
  results = bigquery.query %Q[
    SELECT patient.patientId as id,
           ARRAY_AGG(STRUCT(demographic.MEM_DOB as dob,
                            demographic.MEM_SSN as ssn,
                            demographic.MEM_FNAME as first_name,
                            demographic.MEM_LNAME as last_name)) as demographics
    FROM `emblem-data.silver_claims.member_demographics`
    WHERE patient.patientId IS NOT NULL AND patient.patientId IN UNNEST(@ids)
    GROUP BY id
    ORDER BY id
  ], params: {ids: patients.map { |patient| patient[:id] } }

  patients.each { |patient|
    result = results.bsearch { |r| r[:id] >= patient[:id] }
    patient[:emblem] = result if patient[:id] == result[:id]
  }
end

def fetch_ccds(patients:, api_key:, secret:, destination_name:, destination_id:)
  dump_file = Pathname.new('./dump.json')

  if dump_file.exist?
    dumps = JSON.parse(dump_file.read).sort { |a, b| a[:id] <=> b[:id] }
    patients.each { |patient|
      dump = dumps.find { |dump| dump['id'] == patient[:id] }
      patient[:ccd] = dump[:ccd] if dump
    }
    return
  end

  client = RedoxClient.new(
    api_key: api_key,
    secret: secret,
    source: nil,
    destinations: [
      {
        Name: destination_name,
        ID: destination_id
      }
    ]
  )

  client.fetch_ccds(patients: patients, concurrency: CONCURRENCY)
end

check_environment_variables

patients = fetch_patient_index(project_id: ENV['CITYBLOCK_DATA_PROJECT_ID'])
  .map { |datum|
  { index: datum }
}.select { |patient|
  external_ids = patient[:index].properties['externalIds']
  (external_ids &&
   external_ids.properties['acpny'] &&
   external_ids.properties['acpny'].length > 0)
}

patients.each { |patient|
  patient[:id] = patient[:index].properties['id']
  patient[:mrn] = patient[:index].properties['externalIds'].properties['acpny'][0]
}

fetch_emblem_demographics(patients: patients,
                          project_id: ENV['CITYBLOCK_DATA_PROJECT_ID'])

fetch_ccds(patients: patients,
           api_key: ENV['REDOX_API_KEY'],
           secret: ENV['REDOX_CLIENT_SECRET'],
           destination_name: ENV['REDOX_DESTINATION_NAME'],
           destination_id: ENV['REDOX_DESTINATION_ID'])

File.open('dump.json', 'w') { |file|
  dump = patients.map { |patient|
    { id: patient[:id],
      mrn: patient[:mrn],
      ccd: patient[:ccd],
      emblem: patient[:emblem] }
  }.to_json
  file.write(dump)
}

if ENV['MIXER_ANALYZE']
  patients.each { |patient|
    begin 
      unparsed = patient[:ccd]['Header']['Patient']['Demographics']['DOB']
      patient[:ccd_dob] = Date.parse(unparsed)
    rescue NoMethodError
      STDERR.puts "[patient_id:#{patient[:id]}] Invalid path to CCD DOB"
    end

    begin
      # TODO right now we assume that each emblem demographics instance is the same
      patient[:emblem_dob] = patient[:emblem][:demographics][0][:dob]
    rescue NoMethodError
      STDERR.puts "[patient_id:#{patient[:id]}] Invalid path to Emblem DOB"
    end
  }

  mismatched_patients = patients.select { |patient|
    patient[:ccd_dob] && patient[:emblem_dob] &&
      patient[:ccd_dob] != patient[:emblem_dob]
  }

  File.open('mismatches.json', 'w') { |file|
    dump = mismatched_patients.map { |patient|
      {
        patientId: patient[:id],
        MRN: patient[:mrn],
        CCDDateOfBirth: patient[:ccd_dob],
        EmblemDateOfBirth: patient[:emblem_dob]
      }
    }.to_json
    file.write(dump)
  }

  puts "[verify_patient_demographics] Successfully wrote #{mismatched_patients.length} mismatches to mismatches.json"
end
