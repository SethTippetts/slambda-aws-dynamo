'use strict';

const AWS = require('aws-sdk');
const Bluebird = require('bluebird');
const debug = require('debug')('slambda:storage:Memory');
const uuid = require('uuid');

const Slambda = require('slambda');

const METHOD_TABLE = Slambda.METHOD_TABLE;
const CONTAINER_TABLE = Slambda.CONTAINER_TABLE;

const defaults = {
  region: 'us-east-1',
  tables: {
    [METHOD_TABLE]: 'Slambda-Method',
    [CONTAINER_TABLE]: 'Slambda-Container',
  },
}

module.exports = class Memory {
  constructor(options) {
    debug('constructor');
    this.options = Object.assign({}, defaults, options || {});
    this.service = new AWS.DynamoDB.DocumentClient({ region: this.options.region });
  }

  tableLookup(key) {
    let val = this.options.tables[key];
    console.log(this.options.tables);
    debug(`#tableLookup ${key} : ${val}`)
    return val;
  }

  get(TableName, id) {
    debug(`#get() ${JSON.stringify(arguments)}`);
    return this.service
      .get({
        TableName,
        Key: { id },
      })
      .promise()
      .then(parseItem);
  }

  buildQuery(TableName, index, id) {
    index = index || 'id';
    let query = {
      TableName,
      KeyConditionExpression: `${index} = :hkey`,
      ExpressionAttributeValues: {
        ':hkey': id,
      },
    };
    if (index !== 'id') query.IndexName = `${index}-index`;
    return query;
  }

  query(params) {
    console.log('query', params);
    return this
      .service
      .query(params)
      .promise()
      .then(parseItem);
  }

  findById(TableName, index, id) {
    debug(`#findById() ${JSON.stringify(arguments)}`);
    let params = this.buildQuery(this.tableLookup(TableName), index, id);
    return this.query(params);
  }

  list(TableName) {
    debug(`#list() ${JSON.stringify(arguments)}`);
    return this.service
      .scan({ TableName })
      .promise()
      .then(parseItem)
  }

  put(TableName, Item) {
    debug(`#put() ${JSON.stringify(arguments)}`);
    if (!Item.id) Item.id = uuid.v4();
    replaceEmpty(Item);
    return this.service
      .put({
        TableName,
        Item,
      })
      .promise()
      .then(() => this.get(Item.id));
  }

  delete(TableName, id) {
    debug(`#delete() ${JSON.stringify(arguments)}`);
    return this.service
      .delete({
        TableName,
        Key: { id },
      })
      .promise()
  }
}

function parseItem(response) {
  return response.Item || response.Items;
}

function replaceEmpty(obj) {
  if (obj === null) return;
  for (var prop in obj) {
    let val = obj[prop];
    if (Array.isArray(obj)) obj[prop] = val.filter(v => !isEmpty(v));
    else if (typeof val === 'object') replaceEmpty(val);
    else if (isEmpty(val)) delete obj[prop];
  }
  return obj;
}

function isEmpty(val) {
  return val === '' || val === null || typeof val === 'undefined';
}
