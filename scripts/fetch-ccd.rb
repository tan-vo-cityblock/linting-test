#!/usr/bin/env ruby

require 'pry'

require_relative 'lib/redox_client'

mrn = ARGV[0]

client = RedoxClient.new(
  api_key: ENV['REDOX_API_KEY'],
  secret: ENV['REDOX_CLIENT_SECRET'],
  source: nil,
  destinations: [
    {
      Name: ENV['REDOX_DESTINATION_NAME'],
      ID: ENV['REDOX_DESTINATION_ID'],
    }
  ]
)

patient = {
  mrn: mrn
}

client.fetch_ccds(patients: [patient])

if not patient[:ccd]
  puts "Unable to fetch ccd for MRN: #{mrn}"
else
  puts patient[:ccd].to_json
end
