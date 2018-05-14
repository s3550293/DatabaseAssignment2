echo "compiling code"
javac *.java
echo -e "loading heap file\n"
java hashload -p 4096 BUSINESS_NAMES_201805.csv
echo "querying heap for \"mf engineering\""
java hashquery "mf engineering" 4096