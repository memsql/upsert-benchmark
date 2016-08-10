#
sudo apt-get install python-numpy
sudo pip install cassandra-driver
# Note: this mirror link is volatile. Feel free to grab your own mirror link if need be. 
wget http://mirror.reverse.net/pub/apache/cassandra/3.0.8/apache-cassandra-3.0.8-bin.tar.gz
tar xvf apache-cassandra-3.0.8-bin.tar.gz
sudo touch /etc/apt/sources.list.d/java-8-debian.list
sudo sh -c 'touch /etc/apt/sources.list.d/java-8-debian.list
sudo cat << EOT >> /etc/apt/sources.list.d/java-8-debian.list
deb http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main
deb-src http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main
EOT'
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EEA14886
sudo apt-get update
sudo apt-get install oracle-java8-installer
