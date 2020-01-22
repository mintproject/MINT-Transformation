#!/bin/bash
set -e

#rm -r ../flaskr/static/css
#rm -r ../flaskr/static/js/*.js
#rm ../flaskr/static/precache-manifest*

cp -a build/static/css ../flaskr/static/
cp -a build/static/js ../flaskr/static/
cp -a build/static/media ../flaskr/static/
cp -a build/index.html ../flaskr/templates
cp build/favicon.ico ../flaskr/static
cp build/manifest.json ../flaskr/static
cp build/service-worker.js ../flaskr/static
cp build/asset-manifest.json ../flaskr/static
cp build/precache-manifest* ../flaskr/static
