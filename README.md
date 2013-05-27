d'Exodus - distributed key-value store
======================================
- no installation
- no administration
- no start parameters
- no single point of failure
- friend node auto discovery
- http/json api

Start
=====
Download and unpack dexodus.zip. Then type:
java -jar server.jar

To connect to database cluster, you need to get url from startup logs:

18:15:56.443 [      main] Start server http://172.20.208.215:8082/

API
===
To put key into database:

/get/{ns}/{key}

Where {ns} is namespace name and {key} is a data key.

To get key from database:
/get/{ns}/{key}

To get database friend nodes:
/friends
