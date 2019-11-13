@ECHO OFF
FOR %%I IN (*.jar) DO (
echo ^<dependency^>
echo ^<groupId^>local.dummy^</groupId^>
echo ^<artifactId^>%%I^</artifactId^>
echo ^<version^>0.0.1^</version^>
echo ^<scope^>system^</scope^>
echo ^<systemPath^>${project.basedir}/lib/%%I^</systemPath^>
echo ^</dependency^>
)
