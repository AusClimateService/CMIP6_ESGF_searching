#!/bin/bash
###################
# Written for ACS requests for NCI/oi10 downloads using CleF.
# Chloe Mackallah, CSIRO
###################
module use /g/data/hh5/public/modules
module load conda/analysis3

csv=oi10_requests.csv
rm -f $csv

vars=( pr tas uas vas huss psl )
#exps=( historical ssp126 ssp245 ssp370 ssp585 )
exps=( historical ssp126 ssp370 )
ensmems=( r1i1p1f1 r1i1p1f2 )

if [ ! -f $csv ]; then
  for var in ${vars[@]}; do
    for exp in ${exps[@]}; do
      #for ensmem in ${ensmems[@]}; do
        echo $exp, $var, $ensmem
        clef --missing cmip6 -v $var --frequency mon -e $exp >> $csv #-vl $ensmem >> $csv
      #done
    done
  done
  echo 'cleaning output'
  sed -i '/Available on ESGF but not locally:/d' $csv
  sed -i '/ERROR: No matches found on ESGF/d' $csv
  sed -i '/Everything available on ESGF is also available locally/d' $csv
  sed -i '/^$/d' $csv
fi



exit



containsElement () {
  local e match="$1"
  shift
  for e; do [[ "$e" == "$match" ]] && return 0; done
  return 1
}

found=()
for line in $( cat $csv ); do
  IFS='.'; read -ra linesplit <<< "$line"; unset IFS
  expstring="${linesplit[0]}.${linesplit[1]}.${linesplit[2]}.${linesplit[3]}.${linesplit[4]}"
  if ! containsElement $expstring "${found[@]}"; then
    found+=( $expstring )
  fi
done

echo ${found[@]}
