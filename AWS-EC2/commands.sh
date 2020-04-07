EC2 INSTANCES:



#kibana
chmod 400 /Users/steve/.ssh/ec2-covid.pem
ssh -i "/Users/steve/.ssh/ec2-covid.pem" ec2-user@ec2-3-15-146-99.us-east-2.compute.amazonaws.com

scp  -i "/Users/steve/.ssh/ec2-covid.pem" docker-compose.yml  ec2-user@ec2-3-15-146-99.us-east-2.compute.amazonaws.com:/home/ec2-user/covid





scp  -i "/Users/steve/.ssh/ec2-covid.pem" ./es-node-kibana/elasticsearch.yml  ec2-user@ec2-3-15-146-99.us-east-2.compute.amazonaws.com:/home/ec2-user/covid/


#elasticsearch-master
chmod 400 /Users/steve/.ssh/covid19-kibana.pem
ssh -i "/Users/steve/.ssh/covid19-kibana-2.pem" ec2-user@ec2-13-58-53-1.us-east-2.compute.amazonaws.com

scp  -i "/Users/steve/.ssh/covid19-kibana-2.pem"   ec2-user@ec2-13-58-53-1.us-east-2.compute.amazonaws.com:/home/ec2-user/covid/elasticsearch.yml ./

scp  -i "/Users/steve/.ssh/covid19-kibana-2.pem" ./elasticsearch.yml  ec2-user@ec2-13-58-53-1.us-east-2.compute.amazonaws.com:/home/ec2-user/covid/

scp  -i "/Users/steve/.ssh/covid19-kibana-2.pem"  -r ./certs ec2-user@ec2-13-58-53-1.us-east-2.compute.amazonaws.com:/home/ec2-user/covid/

scp  -i "/Users/steve/.ssh/covid19-kibana-2.pem"   ec2-user@ec2-13-58-53-1.us-east-2.compute.amazonaws.com:/home/ec2-user/covid/certs.zip ./





#elasticsearch-worker1
ssh -i "/Users/steve/.ssh/covid19-kibana-2.pem" ec2-user@ec2-3-135-203-125.us-east-2.compute.amazonaws.com


#push elasticsearch.yml
scp  -i "/Users/steve/.ssh/covid19-kibana-2.pem" ./es-node-1/elasticsearch.yml  ec2-user@ec2-3-135-203-125.us-east-2.compute.amazonaws.com:/home/ec2-user/covid/

#push certs
scp  -i "/Users/steve/.ssh/covid19-kibana-2.pem"  -r ./certs ec2-user@ec2-3-135-203-125.us-east-2.compute.amazonaws.com:/home/ec2-user/covid/






#elasticsearch-worker2
ssh -i "/Users/steve/.ssh/covid19-kibana-2.pem" ec2-user@ec2-18-220-232-235.us-east-2.compute.amazonaws.com

#push elasticsearch.yml
scp  -i "/Users/steve/.ssh/covid19-kibana-2.pem" ./es-node-2/elasticsearch.yml  ec2-user@ec2-18-220-232-235.us-east-2.compute.amazonaws.com:/home/ec2-user/covid/

#push certs
scp  -i "/Users/steve/.ssh/covid19-kibana-2.pem"  -r ./certs ec2-user@ec2-18-220-232-235.us-east-2.compute.amazonaws.com:/home/ec2-user/covid/




#elasticsearch-worker3
ssh -i "/Users/steve/.ssh/covid19-kibana-2.pem" ec2-user@ec2-18-224-63-68.us-east-2.compute.amazonaws.com

#push elasticsearch.yml
scp  -i "/Users/steve/.ssh/covid19-kibana-2.pem" ./es-node-3/elasticsearch.yml  ec2-user@ec2-18-224-63-68.us-east-2.compute.amazonaws.com:/home/ec2-user/covid/

#push certs
scp  -i "/Users/steve/.ssh/covid19-kibana-2.pem"  -r ./certs ec2-user@ec2-18-224-63-68.us-east-2.compute.amazonaws.com:/home/ec2-user/covid/





#elasticsearch-loadbalancer
ssh -i "/Users/steve/.ssh/covid19-kibana-2.pem" ec2-user@ec2-3-22-130-243.us-east-2.compute.amazonaws.com

#push elasticsearch.yml
scp  -i "/Users/steve/.ssh/covid19-kibana-2.pem" ./es-node-loadbalancer/elasticsearch.yml  ec2-user@ec2-3-22-130-243.us-east-2.compute.amazonaws.com:/home/ec2-user/covid/

#push certs
scp  -i "/Users/steve/.ssh/covid19-kibana-2.pem"  -r ./certs ec2-user@ec2-3-22-130-243.us-east-2.compute.amazonaws.com:/home/ec2-user/covid/








#mount partition
lsblk
sudo file -s xvdb
sudo mkfs -t xfs /dev/xvdb
sudo mount /dev/xvdb /data

sudo sysctl -w vm.max_map_count=262144


 sudo yum remove podman-manpages-1.4.2-6.module+el8.1.0+4830+f49150d7.noarch docker                   docker-client                   docker-client-latest                   docker-common                   docker-latest                   docker-latest-logrotate                   docker-logrotate                   docker-selinux                   docker-engine-selinux                   docker-engine

sudo dnf config-manager --add-repo=https://download.docker.com/linux/centos/docker-ce.repo

sudo dnf search docker-ce --showduplicates

sudo dnf install docker-ce-3:18.09.1-3.el7.x86_64

sudo systemctl enable --now docker

sudo usermod -aG docker $USER

newgrp docker 

sudo curl -L "https://github.com/docker/compose/releases/download/1.23.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose

sudo chmod +x /usr/local/bin/docker-compose



sudo yum -y install java-1.8.0-openjdk-devel

export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.242.b08-0.el8_1.x86_64/jre

sudo rpm --import https://artifacts.elastic.co/GPG-KEY-elasticsearch


sudo vi /etc/yum.repos.d/elasticsearch.repo

[elasticsearch]
name=Elasticsearch repository for 7.x packages
baseurl=https://artifacts.elastic.co/packages/7.x/yum
gpgcheck=1
gpgkey=https://artifacts.elastic.co/GPG-KEY-elasticsearch
enabled=0
autorefresh=1
type=rpm-md


sudo dnf install -y --enablerepo=elasticsearch elasticsearch

sudo systemctl enable  elasticsearch

sudo rm -Rf /data/elasticsearch
sudo mkdir /data/elasticsearch
sudo chown elasticsearch:elasticsearch  /data/elasticsearch

sudo rm -Rf /var/log/elasticsearch
sudo mkdir /var/log/elasticsearch
sudo chown elasticsearch:elasticsearch  /var/log/elasticsearch


#follow tutorial at https://www.elastic.co/fr/blog/configuring-ssl-tls-and-https-to-secure-elasticsearch-kibana-beats-and-logstash to configure secure ssl

sudo /usr/share/elasticsearch/bin/elasticsearch-certutil cert ca --pem --in /home/ec2-user/covid/instance.yml --out /home/ec2-user/covid/certs.zip

sudo cp /home/ec2-user/covid/certs/ca/ca.crt /home/ec2-user/covid/certs/ec2-18-220-232-235.us-east-2.compute.amazonaws.com/* /etc/elasticsearch/certs

#follow tutorial at https://www.elastic.co/guide/en/kibana/current/production.html#load-balancing-es to Configure the kibana node or a dedicated node as a Coordinating only node to act as a load balancer.





sudo systemctl restart elasticsearch.service

#enable-security : (see settings in kibana/elasticsearch.yml)
sudo /usr/share/elasticsearch/bin/elasticsearch-setup-passwords interactive

sudo /usr/share/elasticsearch/bin/elasticsearch-setup-passwords auto -u "http://ec2-3-135-203-125.us-east-2.compute.amazonaws.com:9200"

#https://www.elastic.co/guide/en/elasticsearch/reference/7.6/users-command.html
#https://www.elastic.co/guide/en/elasticsearch/reference/current/built-in-roles.html
#https://www.elastic.co/guide/en/elasticsearch/reference/7.6/configuring-security.html
sudo /usr/share/elasticsearch/bin/elasticsearch-users useradd monitor -p @@@PASSWORD@@@ -r kibana_dashboard_only_user
sudo /usr/share/elasticsearch/bin/elasticsearch-users useradd kibana_admin -p @@@PASSWORD@@@ -r superuser

sudo /usr/share/elasticsearch/bin/elasticsearch-users roles kibana_admin -a superuser

docker-compose up --remove-orphans

docker-compose down




# m h dom mon dow command
#hourly elasticsearch restart to free memory
30 * * * * sudo systemctl restart elasticsearch.service