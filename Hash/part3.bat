echo "compiling code"
javac *.java
echo -e "loading heap file\n"
java hashload 4096
REM echo "querying heap for \"mf engineering\""
REM java hashquery "mf engineering" 4096