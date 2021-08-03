FROM ruby:2.6-stretch
RUN apt-get update && apt-get dist-upgrade -y
RUN mkdir -p /etc/ssh
RUN mkdir /scripts
WORKDIR /scripts
COPY . /scripts
COPY hosts.pub /etc/ssh/ssh_known_hosts
RUN bundle install --deployment
