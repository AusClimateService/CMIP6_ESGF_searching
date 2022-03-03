#!/bin/bash
#	Purpose: CMIP6 downscaling - read CSV files with downscaling requirements (provided by Chloe Mackallah); This script 
#	is an update to RCM_data_reqs_v0.5.sh and uses 
#	the script 'cmip6-variable-search.py' developed by Scott Wales "https://gist.github.com/ScottWales/f24a56ca7ad64089e1d5eaf111f9b6df"
#	To execute the script the variable "model" needs to be set! 
#	Author: Alicia Takbash (lead and approval: Chloe Mackallah), CSIRO
#	---------------------------------------------------------------------------------

#	Before executuion, change the permissions on the file to make it executable:
# chmod +x RCM_data_reqs_v0.6.sh
#	Before executuion, convert file to Unix format
# dos2unix RCM_data_reqs_v0.6.sh
#	This bash script can be executed with ./RCM_data_reqs_v0.6.sh 


#	Data requirements for using cmip6-variable-search.py (requires being part of the following CMIP groups: hh5)
module purge 
module use /g/data/hh5/public/modules
module load conda/analysis3
#module list

#	Define the output directory
raw_res=$( pwd )/raw_results
proc_res=$( pwd )/processed_results
final_res=$( pwd )/final_results
synth_res=$( pwd )/synthesised_results
mkdir -p $raw_res
mkdir -p $proc_res
mkdir -p $final_res
mkdir -p $synth_res

#	Set variables: choose the downscaling model, and the experiment
#model=AWRA
model=CCAM+BARPA
#model=BARPA
#model=CCAM
#
if [ ! -z $1 ]; then
  export model=$1
fi
echo "model: $model"

#	Delete all the *.csv files created in the previous execution, as this script appends always to the existing files 
rm -f "$raw_res"/*.csv
rm -f "$proc_res"/*.txt
rm -f "$final_res"/*.txt
rm -f "$synth_res"/*.txt
#exit

# Declare an array of string with type for ssps
declare -a ssps=( "historical" "ssp126" "ssp370" )
#declare -a ssps=( "historical" "ssp126" "ssp245" "ssp370" "ssp585" )

#	Read CSV file (requirements)
INPUT=RCM_data_reqs_${model}_v0.6.csv
INPUTNUM=$(( $( cat $INPUT | wc -l ) - 1 ))
#OLDIFS=$IFS
IFS=',' # comma seperated file

[ ! -f $INPUT ] && { echo "$INPUT file not found"; exit 99; }
# read frequencies and variables (skip the first line/headerline)
sed 1d $INPUT | while read f1 f2 f3 v1 v2 v3
do
    if [[ $f1 == "#"* ]]; then
      continue
    fi
    echo "frequencies: $f1 $f2 $f3"  
	echo "variables: $v1 $v2 $v3"
	# loop through the requirements and do a CleF search 
	for ssp in ${ssps[@]}; do
        echo "experiment: $ssp"
        outfile=${model}_${ssp}_${f1}_${f2}_${f3}_${v1}_${v2}_${v3}.csv
	    python cmip6-variable-search.py --experiment_id $ssp --frequency $f1 $f2 $f3 --variable_id $v1 $v2 $v3 --output $raw_res/$outfile
    done	
done < $INPUT
#IFS=$OLDIFS

#exit
#	Delete files that have been created for the header-line
#rm -f "$outfile_dir"/*#f1*v1*.csv

######################################
# Process searches

for ssp in ${ssps[@]}; do
    fnames=${model}_${ssp}
	echo "processing: $fnames"
    #rm -f $proc_res/$fnames*.txt
	#echo "" >> "$proc_res"/${fnames}_models.txt
	#echo "" >> "$proc_res"/${fnames}_models_and_ensemble.txt
    # pull model/ensemble member matches from search files
	for file in "$raw_res"/${fnames}*.csv 
	do
		if [ -f "${file}" ]; then
		    echo "" >> "$proc_res"/${fnames}_models.txt
		    echo "" >> "$proc_res"/${fnames}_models_and_ensemble.txt
		    echo "### ${file##*/}" >> "$proc_res"/${fnames}_models.txt
		    echo "### ${file##*/}" >> "$proc_res"/${fnames}_models_and_ensemble.txt
			cat $file | while IFS=, read -r mip_era activity_id institution_id source_id experiment_id member_id table_id grid_label version frequency instance_id realm variable_id
			do	
				echo $source_id >> "$proc_res"/${fnames}_models.txt 			
				echo $source_id','$member_id >> "$proc_res"/${fnames}_models_and_ensemble.txt 					
			done
		fi 
		#echo "" >> "$proc_res"/${fnames}_models.txt
		#echo "" >> "$proc_res"/${fnames}_models_and_ensemble.txt
	done
	#
    sed -i "/\bsource_id\b/d" "$proc_res"/${fnames}_models_and_ensemble.txt
	awk '/^$/{flag=1;next}/^$/{flag=0}flag' "$proc_res"/${fnames}_models_and_ensemble.txt | uniq -c >> "$proc_res"/${fnames}_models_and_ensemble_unique_count.txt 
	awk 'NF==1 {print | "sort"} NF!=1 {close ("sort"); print}'  "$proc_res"/${fnames}_models_and_ensemble.txt | uniq >> "$proc_res"/${fnames}_models_and_ensemble_unique_NO_count.txt
	echo "searches_matched (#/${INPUTNUM})	model,ensemble" >> "$final_res"/${fnames}_ensembles_final.txt
	sort "$proc_res"/${fnames}_models_and_ensemble_unique_NO_count.txt | sed '/^\s*#/d;/^\s*$/d;/.csv/d;/source_id/d' | uniq -c | sort -rn >> "$final_res"/${fnames}_ensembles_final.txt
    #
	sed -i "/\bsource_id\b/d" "$proc_res"/${fnames}_models.txt
	awk '/^$/{flag=1;next}/^$/{flag=0}flag' "$proc_res"/${fnames}_models.txt | uniq -c >> "$proc_res"/${fnames}_models_unique_count.txt 
	awk 'NF==1 {print | "sort"} NF!=1 {close ("sort"); print}' "$proc_res"/${fnames}_models.txt | uniq >> "$proc_res"/${fnames}_models_unique_NO_count.txt 
	echo "searches_matched (#/${INPUTNUM})	model" >> "$final_res"/${fnames}_models_final.txt
	sort "$proc_res"/${fnames}_models_unique_NO_count.txt | sed '/^\s*#/d;/^\s*$/d;/.csv/d;/source_id/d' | uniq -c | sort -rn >> "$final_res"/${fnames}_models_final.txt
done

####################################
# Synthesise results
echo "synthesising results"

echo "# Synthesised ESGF search results for ${model}, ${ssps[@]}" > ${synth_res}/synthesised_models.txt
found_mods=()
cat ${final_res}/${model}_*_models_final.txt | sort | uniq -d | grep "^      ${INPUTNUM}" | sort >> ${synth_res}/synthesised_models_tmp.txt
sed -i "s/      ${INPUTNUM} //g" ${synth_res}/synthesised_models_tmp.txt
while read line ; do
  found=true
  for finres in ${final_res}/${model}_*_models_final.txt; do
    #echo ${INPUTNUM} $line `basename $finres`
    if ! grep -q "${INPUTNUM} $line" $finres ; then found=false ; fi
    #echo $found
  done
  if $found ; then 
    echo $line >> ${synth_res}/synthesised_models.txt
  fi
done < ${synth_res}/synthesised_models_tmp.txt
rm -f ${synth_res}/synthesised_models_tmp.txt

echo "# Synthesised ESGF search results for ${model}, ${ssps[@]}" > ${synth_res}/synthesised_ensembles.txt
cat ${final_res}/${model}_*_ensembles_final.txt | sort | uniq -d | grep "^      ${INPUTNUM}" | sort >> ${synth_res}/synthesised_ensembles_tmp.txt
sed -i "s/      ${INPUTNUM} //g" ${synth_res}/synthesised_ensembles_tmp.txt
while read line ; do
  found=true
  for finres in ${final_res}/${model}_*_ensembles_final.txt; do
    #echo ${INPUTNUM} $line `basename $finres`
    if ! grep -q "${INPUTNUM} $line" $finres ; then found=false ; fi
    #echo $found
  done
  if $found ; then 
    echo $line >> ${synth_res}/synthesised_ensembles.txt
  fi
done < ${synth_res}/synthesised_ensembles_tmp.txt
rm -f ${synth_res}/synthesised_ensembles_tmp.txt

exit
