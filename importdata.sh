#!/bin/bash
java -jar importer/target/netcdf-hdfs-importer-0.5-SNAPSHOT-shaded.jar -r hdfs://bbc6.sics.se:26801/FAST/ ~/Documents/Uni/Climate/tasminmax_Amon_EC-EARTH_historical_r2i1p1_195001-201212.nc 

