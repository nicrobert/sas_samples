/* Define some utility macros and set some variables */
filename prolog filesrvc folderpath="/Public" filename="SAS-BigQuery-prolog-2024.sas" ;
%include prolog ;

/* Connect to BigQuery */
libname sasbq bigquery cred_path="&credpath"
   project="sas-gelsandbox" schema="sas_innovate" sql_functions=all
   driver_trace=sql driver_tracefile="&logfilename"
   driver_traceoptions=timestamp ;

/* List BigQuery tables/views */
proc datasets lib=sasbq ;
quit ;

/* See the SQL sent by SAS to BigQuery */
options sastrace=',,,d' sastraceloc=saslog nostsuffix msglevel=i ;
%displayTrace ;

/* Default value for SQLGENERATION - BIGQUERY is present */
proc options option=SQLGENERATION ;
run ;


/****************/
/* SQL Pushdown */
/****************/

/* Process a table - look at the log for in-database pushdown */
proc means data=sasbq.yellow_taxi_trips sum ;
   var fare_amount ;
   class payment_type ;
run ;
%displayTrace ;

/* Process a table - look at the log for in-database pushdown */
proc freq data=sasbq.yellow_taxi_trips ;
   tables passenger_count ;
run ;
%displayTrace ;


/*****************************/
/* SQL Implicit pass-through */
/*****************************/

/* Drop if exists */
proc datasets lib=sasbq nowarn nolist ;
   delete yt_trip_with_borough ;
quit ;
/* Join */
proc sql ;
   create table sasbq.yt_trip_with_borough as 
   select year(a.tpep_pickup_datetime) as year, b.Borough as Pickup_Borough
   from sasbq.yellow_taxi_trips as a, sasbq.yellow_taxi_zone_lookup as b
   where a.PULocationID=b.LocationID ;
quit ;
%displayTrace ;

/* Aggregation */
proc sql ;
   select Pickup_Borough, count(*) as nb_trips
   from sasbq.yt_trip_with_borough
   group by Pickup_Borough ;
quit ;
%displayTrace ;


/*****************************/
/* SQL Explicit pass-through */
/*****************************/

/* Drop if exists */
proc sql ;
   connect using sasbq as sasbq_pt ;
   execute(drop table if exists sas_innovate.yt_trip_distance_groups) by sasbq_pt ;
   disconnect from sasbq_pt ;
quit ;
/* Send a customized query to BigQuery */
proc sql ;
   connect using sasbq as sasbq_pt ;
   execute(
      create table sas_innovate.yt_trip_distance_groups as
         select range_bucket(trip_distance, [1.0, 3.0, 10.0]) trip_distance_group,
                trip_distance,
                datetime_diff(tpep_dropoff_datetime, tpep_pickup_datetime, minute) as diff_minutes,
         from sas_innovate.yellow_taxi_trips ;
   ) by sasbq_pt ;
   disconnect from sasbq_pt ;
quit ;
%displayTrace ;

/* Aggregation */
proc sql ;
   connect using sasbq as sasbq_pt ;
   select * from connection to sasbq_pt (
      select trip_distance_group, count(*) as nb_trips, 
             avg(trip_distance) as avg_distance, avg(diff_minutes) as avg_time
      from sas_innovate.yt_trip_distance_groups group by trip_distance_group
   ) ;
   disconnect from sasbq_pt ;
quit ;
%displayTrace ;


/*************************/
/* Extract BigQuery data */
/*************************/

/* yellow_taxi_trips has 38,208,084 records */
/* Default - would expect 11 hours to download the entire table */
/* scanstringcolumns=yes - XXX to download the entire table */
data extract ;
   set sasbq.yellow_taxi_trips(obs=25000 scanstringcolumns=yes) ;
run ;
%displayTrace ;

/* Using a new option MODE that sets multiple others: */
/*    READ_MODE=STORAGE:     use the Storage API for Google to move data into SAS */
/*    SCANSTRINGCOLUMNS=YES: scan all STRING and BYTE columns to determine the */
/*                           actual maximum length of the columns before loading data */
/*    IGNORE_FEDSQL_OBJECTS=YES */
/* yellow_taxi_trips has 38,208,084 records */
/* Ran in 4:31.23 to download the entire table */
data extract ;
   set sasbq.yellow_taxi_trips(obs=25000 mode=performance) ;
run ;
%displayTrace ;


/*******************************/
/* Load SAS data into BigQuery */
/*******************************/

/* Drop if exists */
proc datasets lib=sasbq nowarn nolist ;
   delete yt_extract ;
quit ;
/* Load in BigQuery */
proc append base=sasbq.yt_extract data=extract(obs=500) ;
run ;
%displayTrace ;

/* Bulk-loading */
/* BULKLOAD is essential in order to have acceptable load performance */
options sastrace=off ;
/* Drop if exists */
proc datasets lib=sasbq nowarn nolist ;
   delete yt_extract ;
quit ;
/* Load in BigQuery */
options sastrace=off ;
proc append base=sasbq.yt_extract(bulkload=yes) data=extract ;
run ;
%displayTrace ;
options sastrace=",,,d" ;


/*******/
/* CAS */
/*******/

cas mysession ;

/* Drop caslib if exists */
proc cas ;
   action table.dropCaslib / caslib="casbq" quiet=true ;
quit ;

/* Define a BigQuery caslib */
caslib casbq datasource=(srctype="bigquery",credfile="&credpath",
   project="sas-gelsandbox",schema="sas_innovate",
   use_information_schema=false,scanstringcolumns=true,readbuff=32767,
   DRIVER_TRACE="SQL",
   DRIVER_TRACEFILE="&dclogpath/&sysuserid._sasdcbq_$SAS_CURRENT_HOST.log",
   DRIVER_TRACEOPTIONS="TIMESTAMP|APPEND") libref=casbq ;

/* List BigQuery tables */
proc casutil incaslib="casbq" ;
   list files ;
quit ;

/* Serial - default */
/* yellow_taxi_trips has 38,208,084 records */
/* Ran in 1:05:54.66 to load the entire table in CAS */
%deleteDCTraceFiles(dclogpath=&dclogpath) ;
proc casutil incaslib="casbq" outcaslib="casbq" ;
   load casdata="yellow_taxi_trips" casout="yellow_taxi_trips"
      where="RatecodeID=4" replace ;
   list tables ;
quit ;
/* Display trace - Analyze the SAS log */
%displayDCTrace(dclogpath=&dclogpath) ;

/* New option MODE=PERFORMANCE */
/* Using a new option MODE that sets multiple others: */
/*    readMode=STORAGE:       use the Storage API for Google to move data into CAS */
/*    scanStringColumns=TRUE: scan all STRING and BYTE columns to determine the */
/*                            actual maximum length of the columns before loading data */
/*    numReadNodes=0:         use all available nodes to load data to CAS */
/*    numWriteNodes=0:        use all available nodes to write to your data source */
/*    ignoreFedSQLObjects=TRUE */
/*    project="" (empty string) */
/* yellow_taxi_trips has 38,208,084 records */
/* Ran in 2:56.41 to load the entire table in CAS */
%deleteDCTraceFiles(dclogpath=&dclogpath) ;
proc casutil incaslib="casbq" outcaslib="casbq" ;
   load casdata="yellow_taxi_trips" casout="yellow_taxi_trips"
      datasourceoptions=(mode="performance")
      where="passenger_count>=3" replace ;
   list tables ;
quit ;
/* Display trace - Analyze the SAS log */
%displayDCTrace(dclogpath=&dclogpath) ;


cas mysession terminate ;
