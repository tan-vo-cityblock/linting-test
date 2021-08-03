require 'geocoder'
require 'geocoder/lookups/base'
require 'geocoder/lookups/geocodio'

module Geocoder::Lookup
  class Geocodio < Base
    private

    # Monkeypatch in support for the hipaa api
    def base_query_url(query)
      path = query.reverse_geocode? ? "reverse" : "geocode"
      "https://api-hipaa.geocod.io/v1.3/#{path}?"
    end

    def query_url_params(query)
      fields = ENV["DATA_FIELDS"] || "census,acs-demographics,acs-economics,acs-families,acs-housing,acs-social"
      {
        api_key: configuration.api_key,
        q: query.sanitized_text,
        fields: fields
      }.merge(super)
    end
  end
end