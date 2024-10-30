#!/bin/bash
auth_file=${1:-$HOME/.rosetta/rosetta_auth}

# check whether file exists
if [ -w $auth_file ]; then
   # show prompt if file exists
   echo "Authorization file $auth_file exists. Overwrite? y/n"
   read ANSWER
   case $ANSWER in
        # show message
        [yY] ) echo "The file will be overwritten" ;;
        # show message and exit
        *) echo "The file will not be overwritten"
        exit 13
   esac
fi

# get user and password
read -p "Institution: " institution
read -p "User: " user
read -s -p "Password: " password
echo

# create authorization token
auth=$user-institutionCode-$institution:$password
auth_64=$(echo -ne "$auth" | base64 | tail -n1);

# create parent directory if it does not exist
mkdir -p ${auth_file%/*}

# output result
echo -n "Authorization: Basic $auth_64" > $auth_file
echo "File $auth_file written successfully"
exit 0
