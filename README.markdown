To deploy on SAP HANA Cloud
===========================
You will need a SAP HANA Cloud account, see http://scn.sap.com/docs/DOC-28197 for details

Words used on this application were got from http://www.gutenberg.org/cache/epub/100/pg100.txt

To run on Jetty
===========================
On root of project just type sbt then container:start, the default port will be 8080, you can access http://localhost:8080
for a welcome message from Spray or http://localhost:8080/wordcount to get a result of Akka Actors processing.

