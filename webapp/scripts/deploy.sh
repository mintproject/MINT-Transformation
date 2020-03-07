#!/bin/bash
set -e

rm -r api/static/css
rm -r api/static/js/*.js
rm api/static/precache-manifest*

cp -a build/static/css api/static/
cp -a build/static/js api/static/
cp -a build/static/media api/static/
cp -a build/index.html api/templates
cp build/favicon.ico api/static
cp build/manifest.json api/static
cp build/service-worker.js api/static
cp build/asset-manifest.json api/static
cp build/precache-manifest* api/static
