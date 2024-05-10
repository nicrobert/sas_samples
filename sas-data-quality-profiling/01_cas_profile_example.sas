cas _all_ terminate ;
cas mySession sessopts=(metrics=true messagelevel=all) ;
options msglevel=i ;

/* Example 1 */

/* Profile and Identity analysis */
proc cas ;
   dataDiscovery.profile /
      algorithm="PRIMARY"
      /* Database table: if the table is not loaded, it will load it on the fly */
      table={caslib="dm_pgdvd" name="film"}
      /* Identity analysis */
      identities= {
         {pattern=".*", type="*", definition="Field Content", prefix="QKB_"}
      }
      multiIdentity=true
      locale="ENUSA"
      qkb="QKB CI 33"
      /* Specifies the number of distinct values to collect for a column before abandoning */
      cutoff=0
      /* Specifies the number of distinct values to report for each column */
      frequencies=10
      /* Specifies the number of maximum and minimum values to report */
      outliers=5
      /* Output table */
      casOut={caslib="casuser" name="film_profiled" replace=true replication=0}
   ;
   /* Print some records of the output table */
   table.fetch /
      table={caslib="casuser" name="film_profiled"} to=200 ;
quit ;

libname casuser cas caslib="casuser" ;
/* Open the resulting tables in SAS Studio */

/* Example 2 */

/* Create a new CASLIB */
caslib dataproc datasource=(srctype=path) path="/gelcontent/data/SAMPLE" ;

/* Assign a SAS Library to a CASLIB */
libname libcas cas caslib="dataproc" ;

/* Load the customers table from the new dataproc caslib */
proc casutil incaslib="dataproc" outcaslib="dataproc" ;
   load casdata="customers.sashdat" casout="customers" copies=0 replace ;
quit ;

/* Profile and Identity analysis */
proc cas ;
   dataDiscovery.profile /
      algorithm="PRIMARY"
      table={caslib="dataproc" name="customers"}
      /* Select only the columns to profile */
      columns={"state"}
      identities= {
         {pattern=".*", type="*", definition="Field Content", prefix="QKB_"}
      }
      multiIdentity=true
      locale="ENUSA"
      qkb="QKB CI 33"
      cutoff=20
      frequencies=10
      outliers=5
      casOut={caslib="dataproc" name="customers_profiled" replace=true replication=0}
   ;
   table.fetch /
      table={caslib="dataproc" name="customers_profiled"} to=200 ;
quit ;


cas mySession terminate ;
