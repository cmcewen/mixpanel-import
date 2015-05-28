FROM cloudgear/ruby:2.2-minimal
MAINTAINER Connor McEwen <connor@trywildcard.com>

RUN mkdir -p /opt/mixpanel
WORKDIR /opt/mixpanel

ADD Gemfile /opt/mixpanel/Gemfile
RUN bundle install

ADD mixpanel_import.rb /opt/mixpanel/mixpanel_import.rb

CMD ruby /opt/mixpanel/mixpanel_import.rb