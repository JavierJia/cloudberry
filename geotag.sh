#!/bin/bash -
#===============================================================================
#
#          FILE: geotag.sh
#
#         USAGE: ./geotag.sh < read stdin > write stdout
#
#   DESCRIPTION:
#
#       OPTIONS: ---
#  REQUIREMENTS: ---
#          BUGS: ---
#         NOTES: ---
#        AUTHOR: Jianfeng Jia (), jianfeng.jia@gmail.com
#  ORGANIZATION: ics.uci.edu
#       CREATED: 04/17/2016 01:06:30 PM PDT
#      REVISION:  ---
#===============================================================================

set -o nounset                              # Treat unset variables as an error

sbt -mem 2048 --error 'set showSuccess := false' "project noah" "run-main edu.uci.ics.cloudberry.noah.TwitterJSONTagToADM\
    -state neo/public/data/state.json\
    -county neo/public/data/county.json \
    -city neo/public/data/city.json "
