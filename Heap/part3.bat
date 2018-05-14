echo "removing header from csv"
tail -n +2 "$1" > BUSINESS_NAMES_201803.csv.nohead
for /f "skip=1 delims=*" %%a in (BUSINESS_NAMES_201803.csv) do (
    echo %%a >> BUSINESS_NAMES_201803.csv.nohead   
)
echo "compiling code"
javac *.java
echo -e "loading heap file\n"
java dbload -p 4096 BUSINESS_NAMES_201803.csv.nohead
echo "querying heap for \"mf engineering\""
java dbquery "mf engineering" 4096