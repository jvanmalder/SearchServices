@ECHO OFF

SETLOCAL EnableDelayedExpansion
SET propertiesFile=..\zeppelin.properties

IF EXIST %propertiesFile% (
   FOR /F "tokens=1* delims==" %%A IN (%propertiesFile%) DO (
      IF "%%A"=="alfresco_repository_protocol" (
         SET alfresco_repository_protocol=%%B
      )

      IF "%%A"=="alfresco_repository_host" (
         SET alfresco_repository_host=%%B
      )

      IF "%%A"=="alfresco_repository_port" (
         SET alfresco_repository_port=%%B
      )
   )

   powershell -Command "(gc ..\conf\shiro.ini) -replace 'REPO_PROTOCOL://REPO_HOST:REPO_PORT', '!alfresco_repository_protocol!://!alfresco_repository_host!:!alfresco_repository_port!' | Out-File ..\conf\shiro.ini"
   powershell -Command "(gc ..\conf\interpreter.json) -replace 'REPO_HOST:REPO_PORT', '!alfresco_repository_host!:!alfresco_repository_port!' | Out-File ..\conf\interpreter.json"

   ECHO "Replaced 'REPO_PROTOCOL' with '!alfresco_repository_protocol!', 'REPO_HOST' with '!alfresco_repository_host!' and 'REPO_PORT' with '!alfresco_repository_port!'"
) ELSE (
    ECHO %propertiesFile% NOT found. )
