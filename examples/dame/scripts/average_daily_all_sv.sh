#!/usr/bin/env bash

./run --config examples/dame/templates/average_daily_all_variables.yml.template -i2 74e6f707-d5e9-4cbd-ae26-16ffa21a1d84 -i1 5babae3f-c468-4e01-862e-8b201468e3b5 -p1 average -p2 day -p3 "2011-01-01 00:00:00" -p4 "2011-01-02 00:00:00" -p5 "P1D" -o1 "output.csv"
