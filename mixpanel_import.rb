require "mixpanel_client"
require "redis"

@redis = Redis.new(:host => ENV['LOGSTASH_URI'], :port => 6379)

@client = Mixpanel::Client.new(api_key: ENV['MIXPANEL_API'], api_secret: ENV['MIXPANEL_SECRET'])
@data = @client.request('export', from_date: (Date.today-1).to_s, to_date: (Date.today-1).to_s)

@data.each do |line| 
	@redis.lpush "logstash", Hash["mixpanel_fields", line, "type", "mixpanel", "@timestamp", Time.at(line["properties"]["time"]).utc.iso8601].to_json
end