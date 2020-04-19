## Overview

Multithreaded client-server system to store a set of key-value pairs.

### Instructions to build
* Make sure latest version of Python3 is installed on your computer.
* Extract the project zip and import the project into the desired IDE.
* Switch to the virtual environment provided to protect your global dependencies.
    * `source venv/bin/activate`
* Install the gRPC modules as follows:
    * `python3 -m pip install grpcio`
    * `python3 -m pip install grpcio-tools`
    * `pip3 install -U protobuf`
    * `pip3 install grpcio-status`
    

### Instructions to run
* Start the desired number of servers by running 
    * `python3 server-app.py <my-port-number> 'comma-seperated-server-ports'`
    * `python3 server-app.py 5010 5000,5010,5020,5030,5040`
* Start the required clients by running 
    * `python3 client-app.py <server-host> <server-port-number>`
    * `python3 client-app.py localHost 5000`
   
##### Example:

```
$ python3 client-app.py localHost 5040
Enter the operation to be performed:
PUT <key> <value>
GET <key>
DELETE <key>
>put sampleKey sampleValue
Enter the operation to be performed:
PUT <key> <value>
GET <key>
DELETE <key>
>get sampleKey
```



