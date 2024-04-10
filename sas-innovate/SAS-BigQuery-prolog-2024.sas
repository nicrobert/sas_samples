/* Log file for Compute */
%let logfilename=/gelcontent/data/LOG/bq.log ;
/* Credentials file for BigQuery */
%let credpath=/gelcontent/keys/gel-sas-user.json ;
/* Set the log file directory for CAS data connectors */
/* it exists on all the cas nodes */
%let dclogpath=/gelcontent/data/LOG ;
%let nblines=0 ;

/* Macro to display the trace files */
%macro displayTrace(logfile=&logfilename) ;
   /* Display trace */
   %global nblines ;
   data _null_ ;
      infile "&logfile" end=eof ;
      input ;
      if index(_infile_,"SQL:")>0 and _n_ > &nblines then put _infile_ ;
      if eof then call symput("nblines",strip(put(_n_,15.))) ;
   run ;
%mend ;

/* Some utility macros to understand multi-node */

/* Macro to delete the trace files */
%macro deleteDCTraceFiles(dclogpath=) ;
   /* Delete the TRACE files */
   caslib log type=path path="&dclogpath" ;
   proc cas ;
      table.fileinfo result=fileresult / caslib="log" allFiles=true path="&sysuserid._sasdcbq_%" ;
      filelist=findtable(fileresult) ;
      do cvalue over filelist ;
         table.deleteSource /
            caslib="log"
            source=cvalue.name
            quiet=true ;
      end ;
   quit ;
   caslib log drop ;
%mend ;

/* Macro to display the trace files from the data connectors */
%macro displayDCTrace(dclogpath=) ;
   /* Display trace */
   data _null_ ;
      length logname oldlogname $ 256 casnode $ 20 ;
      retain oldlogname "dummy" ;
      infile "&dclogpath/&sysuserid._sasdcbq_*.log" filename=logname ;
      input ;
      put ;
      if index(_infile_,"DRIVER SQL")>0 then do ;
         if logname ne oldlogname then do ;
            casnode=scan(scan(scan(logname,-1,"/"),1,"."),-1,"_") ;
            put "*****" ;
            put "LOG FROM CAS NODE: " casnode ;
            put "*****" ;
            oldlogname=logname ;
         end ;
         put _infile_ ;
      end ;
   run ;
%mend ;
