/* Equivalent of the dataDiscovery.profile CAS action, but on data from a SAS library */
proc datametrics data=sashelp.prdsale out=prdsale_profiled frequencies=10 minmax=5 threads=8 ;
   identities locale='ENUSA' def='Field Content' multiidentity ;
run ;
