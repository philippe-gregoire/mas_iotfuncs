@REM Batch to copy quickly from dev to the location of the installed package in conda find_packages
@REM Copies only existing files
xcopy /y /s /d /u %~dp0..\phg_iotfuncs\*.* %CONDA_PREFIX%\Lib\site-packages\phg_iotfuncs\
