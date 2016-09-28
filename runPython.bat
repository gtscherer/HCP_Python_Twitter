@echo off
REM https://help.hana.ondemand.com/help/frameset.htm?49626c9474584bbfa4a936975b851326.html
REM https://github.com/SAP/pyhdb/
REM http://python-twitter.readthedocs.io/en/latest/twitter.html
REM https://dev.twitter.com/rest/reference/get/search/tweets

if exist neo.bat (
    set neoPath=""
) else (
    @echo on
    set /p neoPath="Please enter path to neo script:":
    @echo off
)

%neoPath%neo open-db-tunnel -h hanatrial.ondemand.com -a "your_account_name" -u "your_user_name" -p "your_password" -i "your_schema_name" --background --output json | python pythonMain.py

if exist endSession.bat (
    .\endSession.bat
    del endSession.bat
)