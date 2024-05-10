/* Example of creation of a data mart that stores the results of profiling runs */
/* in a set of tables that will be easier to consume */
/* An example SAS VA report is also available to consume this data mart */

/* Parameters */
%let inputProfileTable=film_profiled ; /* the profiling result table */
%let inCaslib=casuser ; /* the caslib where this table is */

%let outCaslib=public ; /* the caslib where you want to create the profiling data mart */
%let append_master=yes ; /* yes/no if you want to append the results - set no the first time you create the tables */

/* Uncomment if needed */
/* cas mySession sessopts=(caslib="&inCaslib") ; */

libname profsrc cas caslib="&inCaslib" ;
libname profdm cas caslib="&outCaslib" ;

proc cas ;
    table.tableinfo result=profile_dates / caslib="&inCaslib" name="&inputProfileTable" ;  
    saveresult profile_dates dataout=profile_dates ;
run ;
quit ;

data _null_ ;
    set profile_dates ;
    call symput("profileJobDateTime",strip(put(CreateTime,20.))) ;
run ;
%put &profileJobDateTime ;

/* global metrics */
data _null_ ;
    set profsrc.&inputProfileTable(where=(rowId in (1,11,12))) ;
    if charValue="" then charValue="TABLE NOT COMING FROM PERSISTENT STORAGE" ;
    if rowId=1 then call symput('table_nrows',strip(put(count,15.))) ;
    if rowId=11 then call symput('table_source_caslib_name',upcase(strip(charValue))) ;
    if rowId=12 then call symput('table_source_table_name',scan(upcase(strip(charValue)),1)) ;
run ;
%put &table_nrows &table_source_caslib_name &table_source_table_name ;


/* ***************************************************************** */
/* Extract Data Profile results into separate tables                 */
/*                                                                   */
/* This SAS DATA step reads a profile results table created by       */
/* CAS action dataDiscover.profile, which is part                    */
/* SAS Data Preparation 2.2, and extracts the information into the   */
/* following tables make reporting easier:                           */
/*   - profile_results_column_metrics                                */
/*   - profile_results_tableinfo                                     */
/*   - profile_results_frequencies_high                              */
/*   - profile_results_frequencies_low                               */
/*   - profile_results_patterns_high                                 */
/*   - profile_results_patterns_low                                  */
/*   - profile_results_idanalysis                                    */
/*   - profile_results_outliers_high                                 */
/*   - profile_results_outliers_low                                  */
/*                                                                   */
/*                                                                   */
/* DEVELOPMENT/MAINTENANCE HISTORY                                   */
/* DATE        BY          NOTE                                      */
/* 21MAY2018   SNLWIH      Initial version                           */
/* ***************************************************************** */

/* ***************************************************************** */
/*  Copyright (c) 2018 by SAS Institute Inc., Cary, NC 27513 USA     */
/*  ---All Rights Reserved.                                          */
/* ***************************************************************** */


/* ***************************************************************** */
/* Copyright (c) 2018 SAS Institute Inc.                             */
/* All Rights Reserved                                               */
/* U.S. GOVERNMENT RESTRICTED RIGHTS NOTICE                          */
/* Software and accompanying documentation are provided to the U.S.  */
/* government in a transaction subject to the Federal Acquisition    */
/* Regulations with Restricted Rights.  Use, duplication, or         */
/* disclosure of the software by the government is subject to        */
/* restrictions as set forth in FAR 52.227-19 Commercial Computer    */
/* Software-Restricted Rights (June 1987).  The Contractor/Licensor  */
/* is SAS Institute Inc., located at SAS Campus Drive, Cary, North   */
/* Carolina 27513.                                                   */
/* ***************************************************************** */


/* The program assumes the results from the dataDiscovery.profile    */
/* action are stored in a table called profile_results_table and     */
/* that this table is stored in the default library.                 */
/*                                                                   */
/* When this DATA step runs in CAS it takes advantage of the fact    */
/* that a set-statement with a by-statement sorts the rows on all    */
/* the variables listed in the by-statement.                         */
/* If this DATA step is run in the SAS client, the input data needs  */
/* to be sorted first as follows:                                    */
/*   proc sort data=profile_results_table; by ColumnId RowId; run;   */

data profdm.tmp_profdm_column_metrics
        (keep=
            job_run_date table_source_caslib_name table_source_table_name
            column_name datatype unique_threshold_reached
            max_num max_char max_length_char mean_num median_num 
            min_num min_char min_length_char mode_num mode_char mode_count
            blanks_count nulls_count nulls_pct 
            patterns_unique_count patterns_unique_pct
            pk_candidate
            stddev_num stderr_num 
            unique_count unique_pct
            actual_boolean_count actual_date_count actual_integer_count 
            actual_real_count actual_string_count
            actual_column_type actual_column_type_pct
            blanks_pct
            column_phys_width column_phys_position
            format_name format_num_decimals
            table_nrows
         ) 
     profdm.tmp_profdm_tableinfo
         (keep=job_run_date table_source_caslib_name table_source_table_name table_creation_date table_modification_date table_ncolumns table_nrows)
     profdm.tmp_profdm_frequencies_high 
         (keep=job_run_date table_source_caslib_name table_source_table_name column_name column_type freq_value_num freq_value_char freq_count)
     profdm.tmp_profdm_frequencies_low 
         (keep=job_run_date table_source_caslib_name table_source_table_name column_name column_type freq_value_num freq_value_char freq_count)
     profdm.tmp_profdm_patterns_high 
         (keep=job_run_date table_source_caslib_name table_source_table_name column_name column_type pattern_char pattern_count)
     profdm.tmp_profdm_patterns_low
         (keep=job_run_date table_source_caslib_name table_source_table_name column_name column_type pattern_char pattern_count)
     profdm.tmp_profdm_idanalysis
         (keep=job_run_date table_source_caslib_name table_source_table_name column_name column_type id_type id_score)
     profdm.tmp_profdm_outliers_high
         (keep=job_run_date table_source_caslib_name table_source_table_name column_name column_type outlier_value_num outlier_value_char outlier_count)
     profdm.tmp_profdm_outliers_low
         (keep=job_run_date table_source_caslib_name table_source_table_name column_name column_type outlier_value_num outlier_value_char outlier_count)
     / sessref=mysession ;

   /* Use length statement to define column order.                 */
   /* Note that keep= only specifies which variables to output,      */
   /* but does not specify the physical order in the output table    */ 
    length
        job_run_date 8
        table_source_caslib_name $256  
        table_source_table_name $256
        table_creation_date 8
        table_modification_date 8
        table_nrows 8
        table_ncolumns 8
 
        column_name $256
        column_type $8
        datatype $10
        unique_threshold_reached $1
        max_num 8
        max_char $32
        max_length_char 8
        mean_num 8
        median_num 8
        min_num 8
        min_char $256
        min_length_char 8
        mode_num 8
        mode_char $256
        mode_count 8
        blanks_count 8
        blanks_pct 8
        nulls_count 8
        nulls_pct 8
        patterns_unique_count 8
        patterns_unique_pct 8
        pk_candidate $1
        stddev_num 8
        stderr_num 8
        unique_count 8
        unique_pct 8
        actual_boolean_count 8
        actual_date_count 8
        actual_integer_count 8
        actual_real_count 8
        actual_string_count 8
        actual_column_type $32
        actual_column_type_pct 8
        column_phys_width 8
        column_phys_position 8
        format_name $32
        format_num_decimals 8
 
        freq_value_num 8
        freq_value_char $256
        freq_count 8
 
        pattern_char $256
        pattern_count 8
 
        id_type $256
        id_score 8
 
        outlier_value_num 8
        outlier_value_char $256
        outlier_count 8
 
        /* following variables are not output to tables */
        _metric_values_col $32
        _value_num 8
        _value_char $256
        ;
    format
        job_run_date datetime.
        table_creation_date datetime.
        table_modification_date datetime.
        ;
    retain
        column_name column_type datatype unique_threshold_reached
        max_num max_char max_length_char mean_num median_num 
        min_num min_char min_length_char mode_num mode_char mode_count
        blanks_count nulls_count nulls_pct patterns_unique_count patterns_unique_pct
        pk_candidate
        stddev_num stderr_num 
        unique_count unique_pct
        actual_boolean_count actual_date_count actual_integer_count actual_real_count actual_string_count
        actual_column_type actual_column_type_pct
        blanks_pct
        column_phys_width column_phys_position
        format_num_decimals format_name
        _metric_values_col
        ;
    retain
        job_run_date
        table_source_caslib_name table_source_table_name table_creation_date table_modification_date table_ncolumns table_nrows
        ;

    /* Assumptions around Profile Results Table:
        + Records are grouped by ColumnId
        + Records are sorted by RowId within each group of ColumnId
            + This is needed as the algorithm needs to have specific records before it
              can process subsequent record.
            + Here are a few examples of why this is important. 
                + RowId=1003 needs to be found first, otherwise the algorithm does not know
                  in which column to find the metric value.
                + RowId=999 is created if the column had a format associated. If the format was
                  associated with a numeric value, certain metrics are always found in column
                  CharValue, despite the fact that RowId=1003 points to DoubleValue. 
   */
    set profsrc.&inputProfileTable;
    by ColumnId RowId;
    
    /* NIR */
    job_run_date=&profileJobDateTime ;
    table_source_caslib_name="&table_source_caslib_name" ;
    table_source_table_name="&table_source_table_name" ;
    table_nrows=&table_nrows ;

    if first.ColumnId then do;
   
        if ColumnId = -1 then do;
            /* Table related info. No special initialization work to do */ 
        end; 
        else do;
            /* Column related info */
            call missing(column_name, column_type, datatype, unique_threshold_reached,
                         datatype, max_num, max_char, max_length_char, mean_num, median_num, min_num, min_char, min_length_char,
                         mode_num, mode_char, mode_count, blanks_count, nulls_count, nulls_pct, 
                         patterns_unique_count, patterns_unique_pct,
                         pk_candidate, stddev_num, stderr_num, unique_count, unique_pct, 
                         actual_boolean_count, actual_date_count, actual_integer_count, actual_real_count, actual_string_count,
                         actual_column_type, blanks_pct, column_phys_width, column_phys_position, format_num_decimals, format_name
                        );
            call missing(_metric_values_col);
            actual_column_type_pct = 100;
        end;
 
    end; /* if first.ColumnId then do; */
 
    if ColumnId = -1 then do;
        /* Table info, we only capture a subset of the info */
        select (rowid);
            *when(1)  table_nrows = Count;
            when(3)  table_ncolumns = Count;
            when(4)  table_creation_date = Count/1e6; /* CAS datetimes are stored without decimals */
            when(5)  table_modification_date = Count/1e6; /* CAS datetimes are stored without decimals */
            *when(11) table_source_caslib_name = CharValue; /* Note that this field is empty when table was not directly loaded from persisted storage */
            *when(12) table_source_table_name = CharValue; /* Note that this field is empty when table was not directly loaded from persisted storage */
            otherwise; /* Other table info records are ignored */
        end;
    end; /* if ColumnId = -1 then do; */
    else do;
        /* Column related info */
        select (rowid);
            when( 999) format_name = CharValue; /* Note that format w.d for column in CAS table */
                                                /* is currently reports by CAS as Fw.d.               */
            when(1000) column_name = CharValue;
            when(1001) do;
                select(Count);
                    when(1)   datatype = "CHAR";
                    when(2)   datatype = "VARCHAR";
                    when(3)   datatype = "DATE";
                    when(4)   datatype = "DATETIME";
                    when(5)   datatype = "DECQUAD";
                    when(6)   datatype = "DECSEXT";
                    when(7)   datatype = "DOUBLE";
                    when(8)   datatype = "INT32";
                    when(9)   datatype = "INT64";
                    when(10)  datatype = "TIME";
                    otherwise datatype = "_UNKNOWN_";
                end;
                /* Only distinghuish between numeric or character for retrieving metric value */
                if Count in (1,2) then column_type="char";
                else if Count in (5,6,7,8,9) then column_type="num";
                else column_type="_other_"; /* TODO: Handle other data types once CAS supports them */ 
            end;
            when(1002) do;
               /* Skip this metric as it always shows the same value as the number of records in the input table,
                  which is already reported by ColumnId=-1 and RowId=1.
  
                  In future when CAS officially supports arrays of values in a single column, this specific metric
                  might become useful. At that time evaluate the need for this metric.
               */
            end;
            when(1003) do;
                /* Name of the column containing the profiled column metric values. */
                _metric_values_col = CharValue;
            end;
            when(1004) do;
                /* High frequency distribution */
                /* Need special handling for numeric columns with formats.
                   If a numeric column has a format, the frequency value will be stored in CharValue
                   despite the fact that rowid 1003 says DoubleValue or DecSextValue!
                   Therefore check that format_name ne '' (rowid 999) to decide that _value_char needs
                   to be used for numeric columns.
                */ 
  
                link get_metric_value;
  
                select (column_type);
                    when("num") do;
                        if format_name ='' then freq_value_num = _value_num;
                        else do;
                            /* Although profiled column was numeric, a format was associated with it.
                               Therefore the frequency values are in CharValue.
                            */
                            freq_value_char = _value_char;
                            freq_value_num = .;
                        end;
                    end;
                    when("char") do;
                        freq_value_char = _value_char;
                    end;
                    otherwise;
                end; /* select (column_type); */
  
                freq_count = Count;
                output profdm.tmp_profdm_frequencies_high;
            end;
            when(1005) do;
                /* Low frequency distribution */
                /* The low frequency distribution for a specific column only differs from the high
                   frequency distribution if there were more distinct values than the frequency treshold.
                   This is an option on the profile action, frequencies=, and defaults to 1000. The actual
                   value used is not captured in the profile results table.
                   Also here there is a need for special handling for numeric columns with formats. See 
                   rowid=1004 for details.
                */
                link get_metric_value;
  
                select (column_type);
                    when("num") do;
                        if format_name ='' then freq_value_num = _value_num;
                        else do;
                            /* Although profiled column was numeric, a format was associated with it.
                               Therefore the frequency values are in CharValue.
                            */
                            freq_value_char = _value_char;
                            freq_value_num = .;
                        end;
                    end;
                    when("char") do;
                        freq_value_char = _value_char;
                    end;
                    otherwise;
                end; /* select (column_type); */
 
                freq_count = Count;
                output profdm.tmp_profdm_frequencies_low;
            end;
            when(1006) do;
                link get_metric_value;
                if column_type="num" then max_num = _value_num;
                                     else max_char = _value_char;
                end;
            when(1007) max_length_char = Count;
            when(1008) mean_num = DoubleValue; /* Use DoubleValue as SAS Data Quality 3.4 does not include DecSextValue */
            when(1009) median_num = DoubleValue; /* Use DoubleValue as SAS Data Quality 3.4 does not include DecSextValue */
            when(1010) do;
                link get_metric_value;
                if column_type="num" then min_num = _value_num;
                                     else min_char = _value_char;
            end;
            when(1011) min_length_char = Count;
            when(1012) do;
                link get_metric_value;
                if column_type="num" then mode_num = _value_num;
                                     else mode_char = _value_char;
                mode_count = Count;
                /* if mode_count=-1 then mode couldn't be uniquely determined and profile viewer reports as null */
            end;
            when(1013) blanks_count = Count;
            when(1014) nulls_count = Count;
            when(1015) do;
                /* Skip this record. Deprecated in SAS Data Quality 3.4, replaced by RowId=1040. 
                   nulls_pct = Count;
                */
            end;
            when(1016) do;
                 /* High outliers (or better Top xx values). Outliers always report raw values */
                 link get_metric_value;
                 if column_type="num" then outlier_value_num = _value_num;
                                      else outlier_value_char = _value_char;
                 outlier_count = Count;
                 output profdm.tmp_profdm_outliers_high;
            end;
            when(1017) do;
                /* Low outliers (or better Low xx values). Outliers always report raw values */
                link get_metric_value;
                if column_type="num" then outlier_value_num = _value_num;
                                     else outlier_value_char = _value_char;
                outlier_count = Count;
                output profdm.tmp_profdm_outliers_low;
            end;
            when(1018) do;
                /* Pattern high frequency */
                pattern_char = CharValue;
                pattern_count = Count;
                output profdm.tmp_profdm_patterns_high;
            end;
            when(1019) do;
                /* Pattern low frequency */
                pattern_char = CharValue;
                pattern_count = Count;
                output profdm.tmp_profdm_patterns_low;
            end;
            when(1020) patterns_unique_count = Count;
            when(1021) do;
                if count ne 0 then pk_candidate = "Y";
                              else pk_candidate = "N";
            end;
            when(1022) stddev_num = DoubleValue; /* Use DoubleValue as SAS Data Quality 3.4 does not include DecSextValue */
            when(1023) stderr_num = DoubleValue; /* Use DoubleValue as SAS Data Quality 3.4 does not include DecSextValue */
            when(1024) do;
                /* If count ne 100 in record with rowid=1024, then the unique-value-threshold was
                   reached for this input column.
    
                   This can only happen if the primary algorithm was run without fallback to the
                   secondary algorithm. Using the primary algorithm and having too many distinct values 
                   processed by single node, meaning greater than the cutoff= option, will result in a 
                   unique-value-threshold ne 100 situation.
    
                   When a profile is run via the profile microservice than the microservice will fallback to
                   the secondary algorithm for columns that hit the threshold. The SAS Data Explorer UI always
                   uses the profile microservice to run profiles.
    
                   When the unique-value-threshold was reached, certain metrics could not be 
                   calculated correctly. Furthermore, in this case the Frequency Distribution Low and
                   Frequency Distribution High might not be identical.
     
                   TODO:
                    + Consider adding special handling when threshold was reached for a column.
                      See comments in code section starting with "if last.ColumnId then do;". 
                */
                if Count=100 then unique_threshold_reached = "N";
                             else unique_threshold_reached = "Y";
             end;
            when(1025) unique_count = Count;
            when(1026) do;
                /* Skip this record. Deprecated in SAS Data Quality 3.4, replaced by RowId=1041. 
                   unique_pct = Count;
                */   
            end;
            when(1027) do;
                /* Skip this record. Deprecated in SAS Data Quality 3.4, replaced by RowId=1042. 
                   patterns_unique_pct = Count;
                */
            end;
            when(1028) do;
                /* Identity Analysis results.
                   The profile action doesn't use any thresholds to limit the number of records.
                   The amount of records for a column depends on the data itself, and on the
                   number of distinct values returned by the QKB definition(s) specified
                   in the identities= option on the profile action invocation.
                */
                id_type = CharValue;
                id_score = DoubleValue;
                output profdm.tmp_profdm_idanalysis;
            end;
            when(1029) actual_boolean_count = Count;
            when(1030) actual_date_count = Count;
            when(1031) actual_integer_count = Count;
            when(1032) actual_real_count = Count;
            when(1033) actual_string_count = Count;
            when(1034) do;
                select(Count);
                    when(1029) actual_column_type = "BOOLEAN";
                    when(1030) actual_column_type = "DATE";
                    when(1031) actual_column_type = "INTEGER";
                    when(1032) actual_column_type = "REAL";
                    when(1033) actual_column_type = "STRING";
                    otherwise  actual_column_type = "_UNKNOWN_";
                end;
            end;
            when(1035) actual_column_type_pct = Count;
            when(1036) do;
                /* Skip this record. Deprecated in SAS Data Quality 3.4, replaced by RowId=1042. 
                   blanks_pct = Count;
                */
            end;
            when(1037) column_phys_width = Count;
            when(1038) column_phys_position = Count;
            when(1039) format_num_decimals = Count;
   
            /* Note: Rowids 1040-1043 are new in SAS Data Quality 3.4 and replace the deprecated values in rowids 1015, 1026, 1027, 1036.
                     The deprecated rows contained integer values. The new rows represent "high-precision" PCT columns.
                     Note that the "old" rows are still present in the profile results table. 
   
                     As the input data is sorted, these high-precision PCT columns (with higher rowid numbers)
                     will automatically be reported by the extraction routine if the records exist. This
                     because this DATA step applies a "last-one-in wins" principle. 
            */
            when(1040) nulls_pct = DoubleValue; /* Use DoubleValue as SAS Data Quality 3.4 does not include DecSextValue */
            when(1041) unique_pct = DoubleValue; /* Use DoubleValue as SAS Data Quality 3.4 does not include DecSextValue */
            when(1042) pattern_unique_pct = DoubleValue; /* Use DoubleValue as SAS Data Quality 3.4 does not include DecSextValue */
            when(1043) blanks_pct = DoubleValue; /* Use DoubleValue as SAS Data Quality 3.4 does not include DecSextValue */
            
            otherwise do;
                put "WARNING: Unknown RowId found and skipped! RowId = " RowId " ColumnId = " ColumnId; 
            end;
        end; /* select (rowid); */
    end; /* else do; */

    if last.ColumnId then do;
        /* NOTE: Even if a frequency distribution record is the last one in a ColumnId that
                 represents column metrics, then still no special handling is required for this
                 case. Reason being that the frequency record has already been output and only 
                 the column_metrics record need to be output.
        */
        /* TODO: Consider to add following on column level:
             - Add special handling when unique-value-threshold was reached for the column
                 + set the following metrics to missing:
                    rowids: 1004, 1005, 1009*, 1012*, 1016, 1017, 1018, 1019,
                            1020*, 1021*, 1025*, 1026*, 1027*, 1041*, 1042*
                 + rowids marked with '*' represent single value metrics, the others
                   have potentially multiple rows.
                 + For the multiple rows metrics affected, consider adding an
                   "OTHER" row with count=. As this is done in last.ColumnId, it would be
                   added as the last record in the specific results table.
                     - high/low frequency distribution (rowids 1004, 1005) add row with 
                          freq_value_char = "<== OTHER ==>"
                          freq_value_num = .
                          freq_count = .
        */
        if ColumnId = -1 then do;
            output profdm.tmp_profdm_tableinfo;
        end;
        else do;
            output profdm.tmp_profdm_column_metrics;
        end;
    end;

return;

get_metric_value:
    /* Get metric value from correct column as indicated by rowid=1003.
         - Special handling to allow to deal with frequency table for numeric columns that have 
           a format associated with it. In that specific case the column value (not the count) is 
           the formatted value and therefore a character string. This character value is stored in CharValue.
 
       TODO:
           This subroutine was created at a time where the metric values for numeric columns could
           be stored in either DoubleValue or DecSextValue. Now that DecSextValue is not used anymore,
           it might make more sense to remove this subroutine and when assigning values for metric in
           each of the when() sections, directly check for the value of _metrics_values_col and decide
           to take the metric value from either DoubleValue or CharValue. It would also remove the need
           for this trick of always retrieving the value of CharValue, just for handling rowids 1004/1005.
           That special handling could be done inside sections when(1004)/ when(1005). 
    */
    _value_char = CharValue;
    _value_num = DoubleValue;
    /* As SAS Data Quality 3.4 does not use DecSextValue, the following 3 code lines have been removed. */
    /***
    _value_char = CharValue;
    if _metric_values_col = "DoubleValue" then _value_num = DoubleValue;
    else if _metric_values_col = "DecSextValue" then _value_num = DecSextValue;
    else if _metric_values_col = "IntegerValue" then _value_num = IntegerValue;
    ***/
    /* Might even want to consider removing this subroutine and removing _value_char/_value_num. 
       Each case statement that uses link get_metric_value would then reference CharacterValue/DoubleValue 
       directly, based on the value of _metrics_values_col.
    */
return; /* get_metric_value: */

run;

%if %upcase(&append_master=YES) %then %do ;

    proc casutil incaslib="&outCaslib" ;
        list tables ;
    run ;
    quit ;

    data profdm.profdm_tableinfo(append=yes) ;
        set profdm.tmp_profdm_tableinfo ;
    run ;
    data profdm.profdm_column_metrics(append=yes) ;
        set profdm.tmp_profdm_column_metrics ;
    run ;
    data profdm.profdm_frequencies_high(append=yes) ;
        set profdm.tmp_profdm_frequencies_high ;
    run ;
    data profdm.profdm_frequencies_low(append=yes) ;
        set profdm.tmp_profdm_frequencies_low ;
    run ;
    data profdm.profdm_patterns_high(append=yes) ;
        set profdm.tmp_profdm_patterns_high ;
    run ;
    data profdm.profdm_patterns_low(append=yes) ;
        set profdm.tmp_profdm_patterns_low ;
    run ;
    data profdm.profdm_idanalysis(append=yes) ;
        set profdm.tmp_profdm_idanalysis ;
    run ;
    data profdm.profdm_outliers_high(append=yes) ;
        set profdm.tmp_profdm_outliers_high ;
    run ;
    data profdm.profdm_outliers_low(append=yes) ;
        set profdm.tmp_profdm_outliers_low ;
    run ;
    
    proc casutil incaslib="&outCaslib" ;
        list tables ;
        droptable casdata="tmp_profdm_column_metrics" ;
        droptable casdata="tmp_profdm_tableinfo" ;
        droptable casdata="tmp_profdm_frequencies_high" ;
        droptable casdata="tmp_profdm_frequencies_low" ;
        droptable casdata="tmp_profdm_patterns_high" ;
        droptable casdata="tmp_profdm_patterns_low" ;
        droptable casdata="tmp_profdm_idanalysis" ;
        droptable casdata="tmp_profdm_outliers_high" ;
        droptable casdata="tmp_profdm_outliers_low" ;
        list tables ;
    run ;
    quit ;

%end ;
%else %do ;

    /* Purge */
    proc casutil incaslib="&outCaslib" ;
        list tables ;
        droptable casdata="profdm_column_metrics" quiet ;
        droptable casdata="profdm_tableinfo" quiet ;
        droptable casdata="profdm_frequencies_high" quiet ;
        droptable casdata="profdm_frequencies_low" quiet ;
        droptable casdata="profdm_patterns_high" quiet ;
        droptable casdata="profdm_patterns_low" quiet ;
        droptable casdata="profdm_idanalysis" quiet ;
        droptable casdata="profdm_outliers_high" quiet ;
        droptable casdata="profdm_outliers_low" quiet ;
        list tables ;
    run ;
    quit ;

    /* Indexing */
    proc cas ;
       table.index / table={caslib="&outCaslib" name="tmp_profdm_tableinfo"} casout={caslib="&outCaslib" name="idx_profdm_tableinfo" indexVars={"table_source_caslib_name","table_source_table_name","job_run_date"}} ;
       table.index / table={caslib="&outCaslib" name="tmp_profdm_column_metrics"} casout={caslib="&outCaslib" name="idx_profdm_column_metrics" indexVars={"table_source_caslib_name","table_source_table_name","job_run_date"}} ;
       table.index / table={caslib="&outCaslib" name="tmp_profdm_frequencies_high"} casout={caslib="&outCaslib" name="idx_profdm_frequencies_high" indexVars={"table_source_caslib_name","table_source_table_name","job_run_date"}} ;
       table.index / table={caslib="&outCaslib" name="tmp_profdm_frequencies_low"} casout={caslib="&outCaslib" name="idx_profdm_frequencies_low" indexVars={"table_source_caslib_name","table_source_table_name","job_run_date"}} ;
       table.index / table={caslib="&outCaslib" name="tmp_profdm_patterns_high"} casout={caslib="&outCaslib" name="idx_profdm_patterns_high" indexVars={"table_source_caslib_name","table_source_table_name","job_run_date"}} ;
       table.index / table={caslib="&outCaslib" name="tmp_profdm_patterns_low"} casout={caslib="&outCaslib" name="idx_profdm_patterns_low" indexVars={"table_source_caslib_name","table_source_table_name","job_run_date"}} ;
       table.index / table={caslib="&outCaslib" name="tmp_profdm_idanalysis"} casout={caslib="&outCaslib" name="idx_profdm_idanalysis" indexVars={"table_source_caslib_name","table_source_table_name","job_run_date"}} ;
       table.index / table={caslib="&outCaslib" name="tmp_profdm_outliers_high"} casout={caslib="&outCaslib" name="idx_profdm_outliers_high" indexVars={"table_source_caslib_name","table_source_table_name","job_run_date"}} ;
       table.index / table={caslib="&outCaslib" name="tmp_profdm_outliers_low"} casout={caslib="&outCaslib" name="idx_profdm_outliers_low" indexVars={"table_source_caslib_name","table_source_table_name","job_run_date"}} ;
    quit;
    run;

    proc casutil incaslib="&outCaslib" outcaslib="&outCaslib" ;
        list tables ;
        promote casdata="idx_profdm_tableinfo" casout="profdm_tableinfo" ;
        promote casdata="idx_profdm_column_metrics" casout="profdm_column_metrics" ;
        promote casdata="idx_profdm_frequencies_high" casout="profdm_frequencies_high" ;
        promote casdata="idx_profdm_frequencies_low" casout="profdm_frequencies_low" ;
        promote casdata="idx_profdm_patterns_high" casout="profdm_patterns_high" ;
        promote casdata="idx_profdm_patterns_low" casout="profdm_patterns_low" ;
        promote casdata="idx_profdm_idanalysis" casout="profdm_idanalysis" ;
        promote casdata="idx_profdm_outliers_high" casout="profdm_outliers_high" ;
        promote casdata="idx_profdm_outliers_low" casout="profdm_outliers_low" ;
        list tables ;
    run ;
    quit ;

    proc casutil incaslib="&outCaslib" ;
        list tables ;
        droptable casdata="tmp_profdm_column_metrics" ;
        droptable casdata="tmp_profdm_tableinfo" ;
        droptable casdata="tmp_profdm_frequencies_high" ;
        droptable casdata="tmp_profdm_frequencies_low" ;
        droptable casdata="tmp_profdm_patterns_high" ;
        droptable casdata="tmp_profdm_patterns_low" ;
        droptable casdata="tmp_profdm_idanalysis" ;
        droptable casdata="tmp_profdm_outliers_high" ;
        droptable casdata="tmp_profdm_outliers_low" ;
        list tables ;
    run ;
    quit ;
    
%end ;

/* Saving 
proc casutil incaslib="&outCaslib" outcaslib="&outCaslib" ;
    list tables ;
    save casdata="profdm_tableinfo" casout="profdm_tableinfo" ;
    save casdata="profdm_column_metrics" casout="profdm_column_metrics" ;
    save casdata="profdm_frequencies_high" casout="profdm_frequencies_high" ;
    save casdata="profdm_frequencies_low" casout="profdm_frequencies_low" ;
    save casdata="profdm_patterns_high" casout="profdm_patterns_high" ;
    save casdata="profdm_patterns_low" casout="profdm_patterns_low" ;
    save casdata="profdm_idanalysis" casout="profdm_idanalysis" ;
    save casdata="profdm_outliers_high" casout="profdm_outliers_high" ;
    save casdata="profdm_outliers_low" casout="profdm_outliers_low" ;
    list tables ;
run ;
quit ; */

/* Uncomment if needed */
/* cas mySession terminate ; */