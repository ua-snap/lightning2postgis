var request = require('request');
var _ = require('lodash');
var fs = require('fs');
var util = require('util');
var spawn = require('child_process');
var winston = require('winston');
var moment = require('moment');

var logger = new winston.Logger({
  transports: [
    new winston.transports.Console({
      timestamp: function () {
        return moment().format();
      },
      formatter: function (options) {
        // Return string will be passed to logger.
        return (
          options.timestamp() +
          ' ' +
          options.level.toUpperCase() +
          ' ' +
          (options.message ? options.message : '') +
          (options.meta && Object.keys(options.meta).length
            ? '\n\t' + JSON.stringify(options.meta)
            : '')
        );
      },
    }),
  ],
});

var lightningUrl =
  'https://fire.ak.blm.gov/arcgis/rest/services/MapAndFeatureServices/Lightning/MapServer/0/query?where=1%3D1&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=*&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&returnDistinctValues=false&resultOffset=&resultRecordCount=&queryByDistance=&returnExtentsOnly=false&datumTransformation=&parameterValues=&rangeValues=&f=geojson';
var lightningFilePath =
  process.env.LIGHTNING_TEMPFILE || '/tmp/lightning.geojson';
var pgString =
  process.env.LIGHTNING_PG_STRING ||
  'dbname=geoserver host=127.0.0.1 user=geoserver';
var execString =
  'ogr2ogr -f "PostgreSQL" PG:"' +
  pgString +
  '" -overwrite ' +
  lightningFilePath;

var yesterdayLightningUrl =
  'https://fire.ak.blm.gov/arcgis/rest/services/MapAndFeatureServices/Lightning/MapServer/1/query?where=1%3D1&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=*&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&returnDistinctValues=false&resultOffset=&resultRecordCount=&queryByDistance=&returnExtentsOnly=false&datumTransformation=&parameterValues=&rangeValues=&f=geojson';
var yesterdayLightningPath = '/tmp/yesterdayLightning.geojson';
var yesterdayExecString =
  'ogr2ogr -f "PostgreSQL" PG:"' +
  pgString +
  '" -overwrite ' +
  yesterdayLightningPath;

logger.info("Fetching today's upstream lightning data...");
request.get(lightningUrl, function (err, res, body) {
  logger.info('Got a response...');
  if (err) {
    logger.error(err);
  } else {
    try {
      logger.info('Parsing upstream data...');
      var geojson = JSON.parse(body);
      var now = Date.now();
      logger.info('Enhancing upstream data...');
      _.each(geojson.features, function (e, i, l) {
        var millisecondsAgo = now - e.properties.UTCDATETIME;
        var hoursAgo = Math.floor(millisecondsAgo / 3600000);
        l[i].properties.hoursago = hoursAgo;
      });
      logger.info('Writing tempfile GeoJSON...');
      fs.writeFileSync(lightningFilePath, JSON.stringify(geojson), 'utf-8');
      logger.info('Importing into PostGIS...');
      spawn.execSync(execString);
    } catch (e) {
      logger.error(e);
    }
  }
  logger.info("...finished updating today's lightning data!");
});

request.get(yesterdayLightningUrl, function (err, res, body) {
  logger.info("Fetching yesterday's upstream lightning data...");
  logger.info('Got a response...');
  if (err) {
    logger.error(err);
  } else {
    try {
      logger.info('Parsing upstream data...');
      var geojson = JSON.parse(body);
      var now = Date.now();
      logger.info('Enhancing upstream data...');
      _.each(geojson.features, function (e, i, l) {
        var millisecondsAgo = now - e.properties.UTCDATETIME;
        var hoursAgo = Math.floor(millisecondsAgo / 3600000);
        l[i].properties.hoursago = hoursAgo;
      });
      logger.info('Writing tempfile GeoJSON...');
      fs.writeFileSync(
        yesterdayLightningPath,
        JSON.stringify(geojson),
        'utf-8'
      );
      logger.info('Importing into PostGIS...');
      spawn.execSync(yesterdayExecString);
    } catch (e) {
      logger.error(e);
    }
  }
  logger.info("...finished updating yesterday's lightning data!");
});
