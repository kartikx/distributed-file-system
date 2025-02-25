#!/bin/bash

# Run this script from /mp3-g64 folder
GOOS=linux GOARCH=amd64 go build .

declare -a allvms=("fa24-cs425-6401.cs.illinois.edu" "fa24-cs425-6402.cs.illinois.edu" "fa24-cs425-6403.cs.illinois.edu" "fa24-cs425-6404.cs.illinois.edu" "fa24-cs425-6405.cs.illinois.edu" "fa24-cs425-6406.cs.illinois.edu" "fa24-cs425-6407.cs.illinois.edu" "fa24-cs425-6408.cs.illinois.edu" "fa24-cs425-6409.cs.illinois.edu" "fa24-cs425-6410.cs.illinois.edu")

for each_vm_machine in "${allvms[@]}"
do
   scp ./mp3 "kartikr2@${each_vm_machine}:~/mp3-g64"
done
