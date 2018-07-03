#!/bin/bash

# Access granted under MIT Open Source License: https://en.wikipedia.org/wiki/MIT_License
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated 
# documentation files (the "Software"), to deal in the Software without restriction, including without limitation 
# the rights to use, copy, modify, merge, publish, distribute, sublicense, # and/or sell copies of the Software, 
# and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies or substantial portions 
# of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED 
# TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL 
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF 
# CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER 
# DEALINGS IN THE SOFTWARE.
#
#
# Description:  This script is used to deploy and notebook and schedule it as a job. 
#               It will delete a pre-existing job with the same name and hence can be used in a CI/CD pipeline.
#
# Usage: deploy_job.sh -j "job conf file or pattern" [optional in case databricks isn't preconfigured: -r "region" -t "token"] [optional: -p "profile"]
# Example: deploy_job.sh -j "myjob.json" [-r "westeurope" -t "dapi58349058ea5230482058"] [-p "myDBWorkspace"]


set -o errexit
set -o pipefail
set -o nounset
#set -o xtrace

function config_job() {
    # get values from the job config
    db_job_name=$(jq -r '.name' "${jobconf}")
    job_notebook_path=$(jq -r '.notebook_task.notebook_path' "${jobconf}")
    job_notebook_dir=$(jq -r '.notebook_task.notebook_path' "${jobconf}" | cut -d"/" -f2)
    job_notebook_name=$(jq -r '.notebook_task.notebook_path' "${jobconf}" | cut -d"/" -f3)
    echo "     job name: ${db_job_name}"
    echo "     notebook full path: ${job_notebook_path}"
    echo "     notebook folder: ${job_notebook_dir}"
    echo "     notebook name: ${job_notebook_name}"

    # create the directory for the notebooks in the workspace
    echo "creaing a folder in the workspace"
    databricks --profile "${db_cli_profile}" workspace mkdirs "/${job_notebook_dir}/"

    # upload notebook
    echo "uploading notebook"    
    databricks --profile "${db_cli_profile}" workspace import "../../notebooks/${job_notebook_name}.py" "${job_notebook_path}" --language python --overwrite

    jobs=$(set +o pipefail && databricks --profile "${db_cli_profile}" jobs list | grep "${db_job_name}" | cut -d" " -f1)
    for job in $jobs
    do
        # if the job already exists we should delete it before recreating it.
        # it's required since we don't know if the job definition has changed or not
        echo "deleting existing job: $job"    
        databricks --profile "${db_cli_profile}" jobs delete --job-id "${job}"
    done

    # create the job
    echo "creating a new job"    
    databricks --profile "${db_cli_profile}" jobs create --json-file "${jobconf}"
}


#set path
parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd "$parent_path"
echo "working direcoty is: $(pwd)" 

db_region=""
db_token=""
db_job_conf=""
db_cli_profile="DEFAULT"

# assign command line options to variables
while getopts r:t:j:p: option
do
    case "${option}"
    in
        r) db_region=${OPTARG};;
        t) db_token=${OPTARG};;   
        j) db_job_conf=${OPTARG};;   
        p) db_cli_profile=${OPTARG};;   
    esac
done

if [ -z "$db_job_conf" ]; then
    echo "Job configuration file wasn't supplied!"
    exit 1
fi


# configure databricks authentication
db_conf_file=~/.databrickscfg
if [ ! -f $db_conf_file ]; then
    # if the conf file doesn't exist we must have some parameters...
    if [ -z "$db_region" ]; then
    echo "Cluster region wasn't supplied!"
    exit 1
    fi

    if [ -z "$db_token" ]; then
        echo "Access token wasn't supplied!"
        exit 1
    fi

    echo "configurating databricks authentication"
    echo "[${db_cli_profile}]" >> $db_conf_file
    echo "host = https://${db_region}.azuredatabricks.net" >> $db_conf_file
    echo "token = ${db_token}" >> $db_conf_file
    echo ""  >> $db_conf_file
fi


for jobconf in $db_job_conf; do
    echo "start process for job config: ${jobconf}"
    config_job
done
