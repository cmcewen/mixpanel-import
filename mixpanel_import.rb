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
end_of_yesterday = Time.new(now.year, now.month, now.day, 0, 0)
yest = now - 60*60*24
yesterday = Time.new(yest.year, yest.month, yest.day, 0, 0)
week_ago = end_of_yesterday - 60*60*24*7
month_ago = end_of_yesterday - 60*60*24*30
two_ago = end_of_yesterday - 60*60*24*14
three_ago = end_of_yesterday - 60*60*24*21
four_ago = end_of_yesterday - 60*60*24*28
five_ago = end_of_yesterday - 60*60*24*35
last_week = Time.new(week_ago.year, week_ago.month, week_ago.day, 0, 0)
last_month = Time.new(month_ago.year, month_ago.month, month_ago.day, 0, 0)
two_weeks_ago = Time.new(two_ago.year, two_ago.month, two_ago.day, 0, 0)
three_weeks_ago = Time.new(three_ago.year, three_ago.month, three_ago.day, 0, 0)
four_weeks_ago = Time.new(four_ago.year, four_ago.month, four_ago.day, 0, 0)
five_weeks_ago = Time.new(five_ago.year, five_ago.month, five_ago.day, 0, 0)

@aggs = {
				"active": {
					cardinality: { 
						field: 'mixpanel_fields.properties.DistinctID.raw'
					} 
				},
				"active_list": {
					terms: {
						field: 'mixpanel_fields.properties.DistinctID.raw',
						size: 10000
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

@retention_aggs = {
				"active_list": {
					terms: {
						field: 'mixpanel_fields.properties.DistinctID.raw',
						size: 10000
					}
				}
			}

@retention = @eclient.search index: '', search_type: 'count', body: { 
	query: { 
		bool: {
			must: [ 
				{
					match: { 
						event: 'FirstAppOpen' 
					}
				},
				{
					range: {
						"@timestamp": {
							gte: five_weeks_ago.iso8601,
						}
					}
				}
			]
		}	
	}, 
	aggs: { 
		"1 weeks ago": { 
			filter: { 
				range: {
					"@timestamp": {
						gte: two_weeks_ago.iso8601,
						lte: last_week.iso8601
					}
				}
			},
	    aggs: @retention_aggs
		},
		"2 weeks ago": {
			filter: { 
				range: {
					"@timestamp": {
						gte: three_weeks_ago.iso8601,
						lte: two_weeks_ago.iso8601
					}
				}
			},
			aggs: @retention_aggs
		},
		"3 weeks ago": {
			filter: { 
				range: {
					"@timestamp": {
						gte: four_weeks_ago.iso8601,
						lte: three_weeks_ago.iso8601
					}
				}
			},
			aggs: @retention_aggs
		},
		"4 weeks ago": {
			filter: { 
				range: {
					"@timestamp": {
						gte: five_weeks_ago.iso8601,
						lte: four_weeks_ago.iso8601
					}
				}
			},
			aggs: @retention_aggs
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

plain_string =  "\tActive users\tSessions\tAvg session length\tAvg time in app per user\n"

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
  <tr>
    <td align="center">
      <table align="center" width="600" border="0" cellspacing="0" cellpadding="0" style="border:1px solid #ccc;">
        <tr>
        	<td></td>
          <td>1 week ago</td>
          <td>2 weeks ago</td>
          <td>3 weeks ago</td>
          <td>4 weeks ago</td>
        </tr>
        <tr>
        	<td>Installs</td>'


WAUS = []

@data["aggregations"]["Weekly "]["active_list"]["buckets"].each do |bucket|
	WAUS.push(bucket["key"])
end

for num in [1, 2, 3, 4]
	html_string += '<td>' +	@retention["aggregations"][num.to_s + " weeks ago"]["active_list"]["buckets"].length.to_s + '</td>'
end

html_string += '</tr><tr><td>Seen in the past week</td>'

for num in [1, 2, 3, 4]
	count = 0
	@retention["aggregations"][num.to_s + " weeks ago"]["active_list"]["buckets"].each do |bucket|
		count += (WAUS.include?(bucket["key"]) ? 1 : 0)
	end
	html_string += '<td>' + count.to_s + '</td>'
end

html_string += '</tr></table></td>
  </tr>
</table>
</html>'

mailgun_endpoint = "https://api:" + ENV['MAILGUN_API_KEY'] + "@api.mailgun.net/v2/trywildcard.com/messages"

message_params = {
  from: "connor@trywildcard.com",
  to: "all@trywildcard.com",
  subject: "Daily Metrics for " + (Date.today - 1).strftime('%m-%d'),
  text: plain_string,
  html: html_string
}

RestClient.post mailgun_endpoint, message_params