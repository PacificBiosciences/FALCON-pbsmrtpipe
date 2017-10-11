export PYTHONUSERBASE=$(pwd)/LOCAL
export PATH=$(pwd)/LOCAL/bin:$PATH

source /mnt/software/Modules/current/init/bash
module load python/2.7.13-UCS4
module load git

#python get-pip.py --user
