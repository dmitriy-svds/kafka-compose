# Kafka Development Environment
This repository gives you the ability to create a local kafka cluster for development and experimentation (NOT production)

##  Setup
1. Install [docker](https://docs.docker.com/installation/#installation) onto your system 
2. Install [docker-compose](https://docs.docker.com/compose/install/)
3. Download [Confluent Platform](http://confluent.io/downloads/) zip file and unzip in a location of your choice, you'll need this mainly for the kafka command/console tools
4. Osx users will need to carry out this [routing stepup step](./OSX-Routing.md) now and every time they restart their system. (Currently looking for a better solution)

##  Running 
0. This step only applies to linux users, mac users should skip this. you'll need to create local directories (under /tmp/docker) that are linked to directories internally used by the containers. set up all associated local volumes/directories by running this command  
``mkdir -p `grep /tmp/docker docker-compose.yml | cut -d' ' -f6 | cut -d':' -f1 | sort` `` 
0. Ensure docker is running (mac users follow these [steps](https://docs.docker.com/installation/mac/#from-your-command-line))
0. You may need to run ``eval "$(docker-machine env default)"``
0. Start all the needed containers.run ``docker-compose up -d``
0. Running ``docker-compose ps`` or ``docker ps`` should show at least a zookeeper container and 1 or 3 kafka containers

##  Walkthroughs
These walkthroughs are aimed at getting you familiar with kafka.    
They are written with the aim of getting different groups (devs, admins) comfortable using kafka.   

0. [**Basic Walkthrough**](./walkthroughs/basic_walkthrough/README.md) - Aimed at everyone and *everyone* should go through this.
0. **Ruby Walkthrough** - TODO
0. **Clojure Walkthrough** - TODO

##  Stopping
To stop all the containers running ``docker-compose stop``   
To remove all the containers ``docker-compose rm``  

##  Notes
0. Currently the zookeeper and kafka containers use volumes that are on the local filesystem for Linux, but for Macs are inside the boot2docker vm (type `boot2docker ssh` and explore /tmp/docker). These volumes will survive container restarts and should be deleted (and recreated for linux users) if you want your containers to start with clean volumes.
0. If you need to repoint the docker-compose.yml soft link, please shutdown and remove the current configuration first ``docker-compose stop && docker-compose rm``

##  TODO

0. Expose/link the configuration directories of zookeeper and kafka containers the same way the logs and data directories are exposed. This will allow people to further experiment with various configurations
0. Explore using a data/volume container instead of volumes linked to local disk.
