## Alfresco Insight Zeppelin Implementation

This projects takes the Apache Zeppelin project and customizes it for our own purposes.

### Get the code

Git:

   git@git.alfresco.com:search_discovery/InsightZeppelin.git

### Use Maven
1. Build

```
mvn clean install
```

This will create a modified Zeppelin WAR file. Apart from that it will make files available in the target folder for the Docker image.

### Docker
To build the docker image:
```
cd target
docker build -t insightzeppelin:develop .
```

To run the docker image:
```
docker run -p 9090:9090 insightzeppelin:develop
```

### License
Copyright (C) 2005 - 2017 Alfresco Software Limited

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
