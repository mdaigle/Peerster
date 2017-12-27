#!/bin/bash
# Only kill containers, so the network doesn't need to be
# braught back up
containers=$( docker ps -aq )
if [ "$containers" ]; then
  echo "*** stopping containers"
  docker rm -f $containers
fi

# start all containers
echo "*** starting dockers"
docker-compose up -d

# allow correct connection between containers
echo "*** rewriting iptables"
iptables -t nat -F POSTROUTING
iptables -F FORWARD
iptables -P FORWARD DROP
iptables -A FORWARD -m state --state RELATED,ESTABLISHED -j ACCEPT 
iptables -I FORWARD -s 172.16.0.0/24 -d 172.16.0.0/24 -j ACCEPT
iptables -I FORWARD -s 172.16.1.0/24 -j ACCEPT
iptables -I FORWARD -s 172.16.2.0/24 -j ACCEPT

# Print usage
echo "*** to connect to the containers, use one of the following:"
for c in nodea nodeb nodepub; do
  echo "docker exec -ti $c su -"
done
