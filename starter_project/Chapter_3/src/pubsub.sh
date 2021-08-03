#!/bin/bash

usage="$(basename "$0") [-h] args_1 args_2

-- This program creates a pull subscription to a pubsub topic and writes to file and  to the console the messages that are passing by. Upon hitting ctrl+c the subscription is safely deleted.

where:
    -h show this help text
    -args_1 the name of the pubsub topic. This does NOT include the fullpath. Note, when specifying this topic, you must deploy in the correct project.
    -args_2 the name of the subcription object you wish to create. This does NOT include the fullpath"

while getopts ':hs:' option; do
  case "$option" in
    h) echo "$usage"
       exit
       ;;
    :) printf "missing argument for -%s\n" "$OPTARG" >&2
       echo "$usage" >&2
       exit 1
       ;;
   \?) printf "illegal option: -%s\n" "$OPTARG" >&2
       echo "$usage" >&2
       exit 1
       ;;
  esac
done
shift $((OPTIND - 1))

cleanup()
{
        echo "DO NOT HIT CTRL + C." |& tee -a $1.txt
        echo "Deleting Subscription Instance. Time of termination: $(date +"%T")" |& tee -a $1.txt
        gcloud pubsub subscriptions delete $1
        exit 0
}

trap 'cleanup $2' SIGINT SIGTERM

echo "Creating Pubsub Subscription. Time of creation: $(date +"%T")" |& tee -a $2.txt
gcloud pubsub subscriptions create $2 --topic $1 |& tee -a $2.txt
echo "Displaying Pubsub content" |& tee -a $2.txt
while true;
do
        gcloud pubsub subscriptions pull --auto-ack $2 |& tee -a $2.txt;
done