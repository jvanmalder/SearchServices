## Alfresco Insight Engine JDBC Driver

Alfresco Insight Engine JDBC Driver extends Solr's JBDC Driver Implementation and can be used with Apache Zeppelin or any other data exploration or visualization tool that has JDBC support.

### Get the code

``git clone https://git.alfresco.com/search_discovery/InsightJDBC.git``

### Building the artifact

``mvn clean install``

This will create a shaded jar in the ``target`` folder which will be used in Apache Zeppelin or any similar BI tool.

### Configuring Zeppelin
Download Zeppelin from [this link](https://zeppelin.apache.org/download.html "Zeppelin download") and unpack it. To start the application change to the ``bin`` folder and start Zeppelin.

**On all unix like platforms:**

``bin/zeppelin-daemon.sh start``

**On Windows:**

``bin\zeppelin.cmd``

To stop Zeppelin just pass the ``stop`` parameter. I.e.

``bin/zeppelin-daemon.sh stop``

Once the application is started you need to create a new ``Interpreter``.

To do this click on ``anonymous`` link on the top right hand corner and select ``Interpreter``. You can create a new ``Interpreter`` by clicking the ``Create`` button (top right hand corner). Give a name to your new ``Interpreter`` and select the interpreter group ``jdbc``. And then add the following properties:
Assuming, ACS Repository is running on localhost:8080,

``default.url=jdbc:alfresco://localhost:8080?collection=alfresco``

``default.driver=org.apache.solr.client.solrj.io.sql.InsightEngineDriverImpl``

Ensure that the default.user and default.password are set to default username and password for ACS.

The shaded jar from this project needs to be added under the ``Dependencies`` section. It can be either an absolute path or Maven GAV coordinates. After that, save the changes and create a ``Notebook``.
A new notebook can be created from the ``Notebook`` link on the top left corner. Click on the link and then click on ``Create new notebook`` menu link. Give the notebook a name and select the newly created interpreter from the ``Default Interpreter`` drop down. Then click on the ``Create Note`` button. You can now execute SQL queries in your new notebook. Just write your SQL query and click the ``Run`` button.

### Configuring other tools

The configuration might differ in other tools but the JDBC driver should work with them as well. Here are the steps how to configure DbVisualizer:

 - From the toolbar select "Tools -> Driver Manager"
 - In the "Driver Manager" window click the "Create a new Driver" button (that's the green plus icon in the toolbar)
 - Give the new driver a name, e.g. ``Alfresco`` and specify the URL Format, i.e. ``jdbc:alfresco://<connection_string>/?collection=<collection>``. No need to replace ``<connection_string>`` or ``<collection>`` here
 - Next the JDBC driver has to be added. Click the "Open file" button (that's the the yellow folder button) and select the Alfresco JDBC driver
 - A new driver has been added. Close the window and add a new database connection
 - Click the "Create new database connection" button (the button is on the "Databases" tab and is a DB icon with a green plus on it)
 - Enter a connection alias and select the Database Driver, i.e. ``Alfresco``
 - Then enter the Database URL in the popup window, i.e. ``jdbc:alfresco://localhost:8080?collection=alfresco`` and click "Finish"

### License
Copyright (C) 2005 - 2018 Alfresco Software Limited

This file is part of the Alfresco software.
If the software was purchased under a paid Alfresco license, the terms of
the paid license agreement will prevail.  Otherwise, the software is
provided under the following open source license terms:

Alfresco is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

Alfresco is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with Alfresco. If not, see <http://www.gnu.org/licenses/>.
