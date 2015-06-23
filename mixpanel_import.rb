require 'mixpanel_client'
require 'redis'
require 'elasticsearch'
require 'time'
require 'rest-client'

@redis = Redis.new(:host => ENV['LOGSTASH_URI'], :port => 6379)

@client = Mixpanel::Client.new(api_key: ENV['MIXPANEL_API'], api_secret: ENV['MIXPANEL_SECRET'])
@data = @client.request('export', from_date: (Date.today-1).to_s, to_date: (Date.today-1).to_s)

@data.each do |line| 
	@redis.lpush "logstash", Hash["mixpanel_fields", 
		line, "type", "mixpanel", 
		#eastern time so we have to add 4 hours
		"@timestamp", Time.at(line["properties"]["time"] + 4*60*60).iso8601].to_json
end

sleep(1800)

@eclient = Elasticsearch::Client.new host: ENV['ELASTICSEARCH_URI']

now = Time.now
yesterday = Time.new(now.year, now.month, now.day-1, 0, 0)
end_of_yesterday = yesterday + 60*60*24
last_week = Time.new(now.year, now.month, now.day-8, 0, 0)
last_month = Time.new(now.year, now.month-1, now.day, 0, 0)

@aggs = {
				"active": {
					cardinality: { 
						field: 'mixpanel_fields.properties.DistinctID.raw' 
					} 
				},
				"stats": {
					stats: {
						field: 'mixpanel_fields.properties.Length'
					}
				}
			}

@data = @eclient.search index: '', search_type: 'count', body: { 
	query: { 
		bool: {
			must: [ 
				{
					match: { 
						event: 'Session' 
					}
				},
				{
					range: {
						"@timestamp": {
							gte: last_month.iso8601,
						}
					}
				}
			]
		}	
	}, 
	aggs: { 
		"Daily  ": { 
			filter: { 
				range: {
					"@timestamp": {
						gte: yesterday.iso8601,
						lte: end_of_yesterday.iso8601
					}
				}
			},
	    aggs: @aggs
		},
		"Weekly ": {
			filter: { 
				range: {
					"@timestamp": {
						gte: last_week.iso8601,
						lte: end_of_yesterday.iso8601
					}
				}
			},
			aggs: @aggs
		},
		"Monthly": {
			filter: { 
				range: {
					"@timestamp": {
						gte: last_month.iso8601,
						lte: end_of_yesterday.iso8601
					}
				}
			},
			aggs: @aggs
		}
	}
}

html_string = '<html>
<table width="600" style="border:1px solid #333">
  <tr>
    <td align="center">
      <table align="center" width="600" border="0" cellspacing="0" cellpadding="0" style="border:1px solid #ccc;">
        <tr>
        	<td></td>
          <td>Active users</td>
          <td>Sessions</td>
          <td>Avg session length</td>
          <td>Avg time in app</td>
        </tr>'

plain_string =  "\tActive users\tSessions\tAvg session length\tAvg time in app\n"

for day in ["Daily  ", "Weekly ", "Monthly"]
	html_string += '<tr><td>' + day + '</td><td>' + 
		@data["aggregations"][day]["active"]["value"].to_s + "</td><td>" + 
		@data["aggregations"][day]["doc_count"].to_s + "</td><td>" + 
		@data["aggregations"][day]["stats"]["avg"].round(2).to_s + " sec </td><td>" + 
		(@data["aggregations"][day]["stats"]["sum"] / @data["aggregations"][day]["active"]["value"]).round(2).to_s + " sec</td></tr>"

	plain_string += day + "\t" + 
		@data["aggregations"][day]["active"]["value"].to_s + "\t\t" + 
		@data["aggregations"][day]["doc_count"].to_s + "\t\t" + 
		@data["aggregations"][day]["stats"]["avg"].round(2).to_s + " sec \t\t" + 
		(@data["aggregations"][day]["stats"]["sum"] / @data["aggregations"][day]["active"]["value"]).round(2).to_s + " sec\n"
end

html_string += '</table></td>
  </tr>
</table>
</html>'

mailgun_endpoint = "https://api:" + ENV['MAILGUN_API_KEY'] + "@api.mailgun.net/v2/trywildcard.com/messages"

message_params = {
  from: "support@trywildcard.com",
  to: "connor@trywildcard.com",
  subject: "Daily Metrics for " + (Date.today - 1).strftime('%m-%d'),
  text: plain_string,
  html: html_string
}

RestClient.post mailgun_endpoint, message_params