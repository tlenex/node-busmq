#!/bin/sh

browserify --standalone busmq --entry . > busmq.js
uglifyjs busmq.js > busmq.min.js
