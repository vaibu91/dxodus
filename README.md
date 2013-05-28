d'Exodus - distributed key-value store
======================================
- no installation
- no administration
- no start parameters
- no single point of failure
- friends node auto discovery
- http/json api

Start
=====
Download and unpack **dexodus.zip**. Then type:
        
    java -jar server.jar

To connect to database cluster, you need to get url from startup logs:

    18:15:56.443 [      main] Start server http://172.20.208.215:8082/

API
===
To get key value from database: `HTTP GET /get/{ns}/{key}` where {ns} as a namespace name and {key} is a data key.

    $.ajax("http://host:port/myns/mykey").done(function(data) {
        window.alert(data);
    })

To put key,value into database: `HTTP POST /get/{ns}/{key}` with form encoded param "value".

    $.ajax({url: "http://host:port/myns/mykey",
        type: "POST",
        data: {
          value: "myvalue"
        }
    })

To get database friend nodes:
/friends
