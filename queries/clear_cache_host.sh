#!/bin/bash

HOSTLIST="141.2.2.172 141.2.2.171 141.2.2.170 141.2.2.169"
for HOST in $HOSTLIST; do
  # description of the command --> http://unix.stackexchange.com/questions/87908/how-do-you-empty-the-buffers-and-cache-on-a-linux-system
  ssh -t -t $HOST 'echo "free && sync && echo 3 > /proc/sys/vm/drop_caches && free"|sudo su' &
done