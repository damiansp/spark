DATA=/Users/damiansp/Learning/spark/data
mkdir $DATA/taxidata
cd $DATA/taxidata
curl -O https://storage.googleapis.com/aas-data-sets/trip_data_1.csv.zip
unzip trip_data_1.csv.zip
head -n 10 trip_data_1.csv

#hadoop fs -mkdir taxidata
#hadoop fs -put trip_data_1.csv taxidata/

cd -
