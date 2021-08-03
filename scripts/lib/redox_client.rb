require 'faraday'
require 'typhoeus'
require 'typhoeus/adapters/faraday'
require 'json'

class RedoxClient
  def initialize(api_key:, secret:, source:, destinations:)
    @api_key = api_key
    @secret = secret
    @source = source
    @destinations = destinations
  end

  def fetch_ccds(patients:, concurrency:1, test:true)
    concurrency = 1 if concurrency < 1
    token = access_token # access_token is not threadsafe

    manager = Typhoeus::Hydra.new(max_concurrency: concurrency)
    conn.in_parallel(manager) do
      patients.map { |patient|
        patient[:response] = conn.post { |request|
          request.url '/endpoint'
          request.headers['Content-Type'] = 'application/json'
          request.headers['Authorization'] = "Bearer #{token}"
          request.body = meta(data_model: 'Clinical Summary',
                              event_type: 'PatientQuery',
                              id: patient[:mrn],
                              id_type: 'MR').to_json
        }
      }
    end

    if patients.map { |patient| patient[:response] }.any? { |response| response.body == 'Invalid request' }
      raise 'Invalid request'
    end
    
    patients.map { |patient|
      begin
        patient[:ccd] = JSON.parse(patient[:response].body)
      rescue TypeError => e
        STDERR.puts 'JSON parsing error: ' + e.to_s
      end
    }
  end

  private

  def meta(data_model:, event_type:, id:, id_type:, test: true)
    {
      Meta: {
        DataModel: data_model,
        EventType: event_type,
        Test: test,
        Destinations: @destinations
      },
      Patient: {
        Identifiers: [
          {
            ID: id,
            IDType: id_type
          }
        ]
      }        
    }
  end

  # This function requires a lock.
  def access_token
    return @auth['access_token'] if @auth && @auth['access_token']

    response = conn.post { |request|
      request.url '/auth/authenticate'
      request.headers['Content-Type'] = 'application/json'
      request.body = {
        :apiKey => @api_key,
        :secret => @secret
      }.to_json
    }

    body = JSON.parse(response.body)

    @auth = {}
    @auth['access_token'] = body['accessToken']
    @auth['expires'] = body['expires']
    @auth['refresh_token'] = body['refreshToken']

    @auth['access_token']
  end

  def conn
    return @conn if @conn

    @conn = Faraday.new(:url => 'https://api.redoxengine.com/') { |faraday|
      faraday.response :logger
      faraday.adapter :typhoeus
    }
  end
end
