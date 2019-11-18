#!/bin/bash
set -e

rm -r ../data/www/css
rm -r ../data/www/js/*.js
rm ../data/www/precache-manifest*

cp -a build/static/css ../data/www/
cp -a build/static/js ../data/www/
cp -a build/index.html ../pkb/templates
cp build/favicon.ico ../data/www/
cp build/manifest.json ../data/www/
cp build/service-worker.js ../data/www/
cp build/asset-manifest.json ../data/www/
cp build/precache-manifest* ../data/www/
