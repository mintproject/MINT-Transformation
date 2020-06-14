#!/usr/bin/env bash

./run --config examples/dame/templates/topoflow_climate.yml.template -i1 5babae3f-c468-4e01-862e-8b201468e3b5 -p1 "2014-08-01 00:00:00" -p2 "2014-08-02 00:00:00" -p3 "atmosphere_water__rainfall_mass_flux" -p4 "23.995416666666,6.532916666667,28.020416666666,9.566250000000" -p5 30 -p6 30 -p7 3600 -o1 outputs1.zip
