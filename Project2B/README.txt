How to build
============

Go to the Project2B directory and type

mvn clean install assembly:single

This will build the resourcemonitor-jar-with-dependencies.jar inside target directory. Copy the resourcemonitor-jar-with-dependencies.jar file to the bin directory

How to run
==========

To start the daemon run the daemon.sh file in the bin directory

i.e daemon.sh 1

When you run the daemon.sh you have to give a unique id.

To start the client run the client.sh file in the bin directory



