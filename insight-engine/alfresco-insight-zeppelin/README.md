## Alfresco Insight Zeppelin Implementation

This projects customizes Apache Zeppelin and uses it as a client for Alfresco Insight Engine.

### Login credentials

Alfresco Insight Zeppelin uses Alfresco Repository for authentication so any existing user in Alfresco can access Alfresco Insight Zeppelin as well

### Get the code
Git:

<code>
git clone git.alfresco.com:search_discovery/InsightZeppelin.git
</code>

### Use Maven
Build project:

<code>
mvn clean install
</code>

This will generate the following artifacts in the 'target' folder. 
1. alfresco-insight-zeppelin zip file:  see /target
2. docker-resources: A folder containing the files necessary for a Docker image: see 'target/docker-resources'

### Start Alfresco Insight Zeppelin from a zip file

* Get the zip file and unzip it
* A file called 'zeppelin.properties' in ZEPPELIN\_HOME/ contains the connection details to Alfresco Repository. Please change the values to match your Alfresco host and port.
* Run the substituter.sh script (or substituter.cmd for Windows) from ZEPPELIN\_HOME/bin/
* Start your Zeppelin Server
    * On all Unix like platforms: ZEPPELIN\_HOME/bin/zeppelin-daemon.sh start
    * On Windows: ZEPPELIN\_HOME\bin\zeppelin.cmd
* After Zeppelin has started successfully, go to http://localhost:9090/zeppelin
* Login with your Alfresco admin user credentials (e.g. admin/admin). Any user in Alfresco can access Zeppelin with the same credentials
* Create a new notebook or use the notebooks provided
* New sql queries can be added to the notebook. Just remember to start the query with %alfresco when using alfresco interpreter.
    * For example,
    * %alfresco select distinct(site) from alfresco
* Stopping Zeppelin
    * On all Unix like platforms: ZEPPELIN\_HOME/bin/zeppelin-daemon.sh stop
    * On Windows: Ctrl + C

**NOTE:** The port number, context path or other properties can be changed in ZEPPELIN\_HOME/conf/zeppelin-env.sh (ZEPPELIN_HOME\conf\zeppelin-env.cmd for Windows). A full list of properties can be found here: https://zeppelin.apache.org/docs/0.7.3/install/configuration.html

By default Zeppelin will use Alfresco to authenticate users. That means every user in Alfresco will be able to access Zeppelin. You can limit the number of user by changing the security manager configuration for authentication in ZEPPELIN\_HOME/conf/shiro.ini. To do that simply comment out all the "alfrescoRealm" related configuration. You can either configure your LDAP or AD to have a group of people which can access Zeppelin. Below there is a configuration sample for AD. With that configuration only user in "ZeppelinUsers" group will have access to the application:

```
ldapRealm = org.apache.zeppelin.realm.LdapRealm
ldapRealm.contextFactory.systemUsername = <principal>
ldapRealm.contextFactory.systemPassword = <password>
ldapRealm.searchBase = OU=Users,DC=test,DC=com
ldapRealm.userSearchFilter = (&(objectclass=person)(sAMAccountName={0})(memberOf:=CN=ZeppelinUsers,OU=Users,DC=test,DC=com))
ldapRealm.userSearchScope = subtree
ldapRealm.authorizationEnabled = true
ldapRealm.contextFactory.url = <ldap-url>
ldapRealm.userSearchAttributeName = sAMAccountName
ldapRealm.contextFactory.authenticationMechanism = simple
ldapRealm.userObjectClass = person
ldapRealm.groupObjectClass = group
ldapRealm.memberAttribute = member
securityManager.realms=$ldapRealm
```

Ideally Zepplin should be deployed on a separate server on its own. If SSL has been configured on Alfresco Repository an extra property has to be added in Zeppelin. This can be done by the "admin" user either in the UI (to do this please go the Interpreter page) or directly in the ZEPPELIN\_HOME/conf/interpreter.json file (please open the file and add a new line after "default.url"). The property which needs to be added is called "alfresco.enable.ssl" and should have the value "true".

### Docker
To build the docker image:

<code>
cd target/docker-resources

docker build -t insightzeppelin:master .
</code>

To run the docker image:

<code>
docker run -e REPO_PROTOCOL=https -e REPO_HOST=myhost -e REPO_PORT=8082 -p 9090:9090 insightzeppelin:master
</code>

Access Alfresco Insight Zeppelin:

<code>
http://localhost:9090/zeppelin
</code>

When building the docker image the environment variables have to be set in the command line like above when the default values should be overridden. 
Default values are: 'REPO\_PROTOCOL=http REPO\_HOST=localhost REPO\_PORT=8080'
When using docker-compose the following configuration can be used:

```
zeppelin:
  image: insightzeppelin:master
  environment:
    - REPO_PROTOCOL=https
    - REPO_HOST=myhost
    - REPO_PORT=8082
  ports:
    - "9090:9090"
```

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