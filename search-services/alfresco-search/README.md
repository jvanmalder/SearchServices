# Alfresco Solr Application
TODO Add project description.
## How to build
Build:
```
mvn clean install
```

## How to test
```
mvn test 
or
mvn test -Dtest={testname}
```

### Project Layout

```
.
├── /README.md                  # Readme file.
├── /pom.xml                    # Project configuration and meta data.
├── /src                        # Project folder.
|   ├── /main                   # Main application folder.
|   |    ├── /java              # Application code.
|   |    ├── /resources         # Application resource.
|   |    |   ├── /solr          # Solr resource.
|   |    ├── /webapp            # Web application code.
|   ├── /test/                  # Test folder.
|   |    ├── /java              # Java test folder.
|   |    ├── /resources         # Test resource directory.
|   |    |   ├── /test-files    # Solr test data directory and test solr location.
```
