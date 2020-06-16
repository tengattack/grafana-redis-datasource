var _ = require('lodash')
var express = require('express')
var bodyParser = require('body-parser')
var redis = require('redis')
var splitargs = require('splitargs')
var assert = require('assert')
var config = require('config')
var Stopwatch = require("statman-stopwatch")

var COMMANDS = [
  'GET', 'MGET',
  'HGET', 'HMGET', 'HGETALL',
  'LINDEX', 'LRANGE',
  'SMEMBERS', 'SDIFF', 'SINTER', 'SUNION',
  'ZRANGE', 'ZREVRANGE', 'ZRANGEBYLEX', 'ZREVRANGEBYLEX', 'ZRANGEBYSCORE', 'ZREVRANGEBYSCORE',
]

var app = express()
app.use(bodyParser.json())

// Called by test
app.all('/', function (req, res, next)
{
  logRequest(req.body, "/")
  setCORSHeaders(res)

  var client = redis.createClient(req.body.db.url)
  client.on('connect', function () {
    res.send({ status : "success",
               display_status : "Success",
               message : 'Redis Connection test OK' })
    client.quit()
    next()
  })
  client.on('error', function (err) {
    res.send({ status : "error",
               display_status : "Error",
               message : 'Redis Connection Error: ' + err.message })
    client.quit()
    next()
  })
})

// Called by template functions and to look up variables
app.all('/search', function (req, res, next)
{
  logRequest(req.body, "/search")
  setCORSHeaders(res)

  // Generate an id to track requests
  const requestId = ++requestIdCounter
  // Add state for the queries in this request
  var queryStates = []
  requestsPending[requestId] = queryStates
  // Parse query string in target
  queryArgs = parseQuery(req.body.target, {})
  if (queryArgs.err != null) {
    queryError(requestId, queryArgs.err, next)
  } else {
    doTemplateQuery(requestId, queryArgs, req.body.db, res, next)
  }
})

// State for queries in flight. As results come it, acts as a semaphore and sends the results back
var requestIdCounter = 0
// Map of request id -> array of results. Results is
// { query, err, output }
var requestsPending = {}

// Called when a query finishes with an error
function queryError(requestId, err, next)
{
  // We only 1 return error per query so it may have been removed from the list
  if (requestId in requestsPending) {
    // Remove request
    delete requestsPending[requestId]
    // Send back error
    next(err)
  }
}

// Called when query finished
function queryFinished(requestId, queryId, results, res, next)
{
  // We only 1 return error per query so it may have been removed from the list
  if (requestId in requestsPending) {
    var queryStatus = requestsPending[requestId]
    // Mark this as finished
    queryStatus[queryId].pending = false
    queryStatus[queryId].results = results

    // See if we're all done
    var done = true
    for (var i = 0; i < queryStatus.length; i++) {
      if (queryStatus[i].pending == true) {
        done = false
        break
      }
    }

    // If query done, send back results
    if (done) {
      // Concatenate results
      var output = []
      for (var i = 0; i < queryStatus.length; i++) {
        var queryResults = queryStatus[i].results
        var keys = Object.keys(queryResults)
        for (var k = 0; k < keys.length; k++) {
          var tg = keys[k]
          output.push(queryResults[tg])
        }
      }
      res.json(output)
      next()
      // Remove request
      delete requestsPending[requestId]
    }
  }
}

// Called to get graph points
app.all('/query', function (req, res, next)
{
    logRequest(req.body, "/query")
    setCORSHeaders(res)

    // Parse query string in target
    // TODO: support substitutions
    substitutions = { '$from' : req.body.range.from,
                      '$to' : req.body.range.to,
                     }

    // Generate an id to track requests
    var requestId = ++requestIdCounter
    // Add state for the queries in this request
    var queryStates = []
    requestsPending[requestId] = queryStates
    var error = false

    for (var queryId = 0; queryId < req.body.targets.length && !error; queryId++) {
      var tg = req.body.targets[queryId]
      queryArgs = parseQuery(tg.target, substitutions)
      queryArgs.type = tg.type
      if (queryArgs.err != null) {
        queryError(requestId, queryArgs.err, next)
        error = true
      } else {
        // Add to the state
        queryStates.push({ pending : true })

        // Run the query
        runQuery(requestId, queryId, req.body, queryArgs, res, next)
      }
    }
  }
)

app.use(function (error, req, res, next)
{
  // Any request to this server will get here, and will send an HTTP
  // response with the error message
  res.status(500).json({ message: error.message })
})

// Get config from server/default.json
var serverConfig = config.get('server')

app.listen(serverConfig.port, serverConfig.host)

console.log("Server is listening on port " + serverConfig.port)

function setCORSHeaders(res)
{
  res.setHeader("Access-Control-Allow-Origin", "*")
  res.setHeader("Access-Control-Allow-Methods", "POST")
  res.setHeader("Access-Control-Allow-Headers", "accept, content-type")
}

function parseQuery(query, substitutions)
{
  var doc = {}
  var queryErrors = []

  query = query.trim()
  var args = splitargs(query)
  if (args.length < 2) {
    queryErrors.push("Query must have command and arguments")
    return null
  }

  // Query is of the form <command> ...arguments
  // Check command first.
  var command = args.shift().toUpperCase()
  if (!COMMANDS.includes(command)) {
    queryErrors.push("Unknown command " + command + "")
  } else {
    doc.command = command
    doc.args = args
  }

  if (queryErrors.length > 0) {
    doc.err = new Error('Failed to parse query - ' + queryErrors.join(':'))
  }

  return doc
}

// Run a query. Must return documents of the form
// { value : 0.34334, ts : <epoch time in seconds> }
function runQuery(requestId, queryId, body, queryArgs, res, next)
{
  var client = redis.createClient(body.db.url)
  client.on('connect', function () {
    logQuery(queryArgs.command, queryArgs.args)
    var stopwatch = new Stopwatch(true)
    var args = queryArgs.args.concat(function (err, result) {
      if (err) {
        client.quit()
        queryError(requestId, err, next)
        return
      }
      client.quit()
      var results, elapsedTimeMs
      try {
        results = getTableResults(queryArgs.command, queryArgs.args, result)
        elapsedTimeMs = stopwatch.stop()
      } catch (err) {
        queryError(requestId, err, next)
        return
      }
      logTiming(queryArgs, elapsedTimeMs)
      // Mark query as finished - will send back results when all queries finished
      queryFinished(requestId, queryId, results, res, next)
    })
    client[queryArgs.command].apply(client, args)
  })
  client.on('error', function (err) {
    client.quit()
    queryError(requestId, err, next)
  })
}

function getTableResults(command, args, result)
{
  var columns = []
  var rows = []
  switch (command) {
  case 'GET':
    columns = [ 'key', 'value' ]
    rows.push([ args[0], result ])
    break
  case 'MGET':
    columns = [ 'key', 'value' ]
    for (var i = 0; i < args.length; i++) {
      rows.push([ args[i], result[i] ])
    }
    break

  case 'HGET':
    columns = [ 'key', 'field', 'value' ]
    rows.push([ args[0], args[1], result ])
    break
  case 'HMGET':
    columns = [ 'key', 'field', 'value' ]
    for (var i = 1; i < args.length; i++) {
      rows.push([ args[0], args[i], result[i - 1] ])
    }
    break
  case 'HGETALL':
    columns = [ 'key', 'field', 'value' ]
    for (var field in result) {
      rows.push([ args[0], field, result[field] ])
    }
    break

  case 'LINDEX':
    columns = [ 'key', 'value' ]
    rows.push([ args[0], result ])
    break
  case 'LRANGE':
    columns = [ 'key', 'value' ]
    for (var i = 0; i < result.length; i++) {
      rows.push([ args[0], result[i] ])
    }
    break

  case 'SMEMBERS':
  case 'SDIFF':
  case 'SINTER':
  case 'SUNION':
    columns = [ 'member' ]
    for (var i = 0; i < result.length; i++) {
      rows.push([ result[i] ])
    }
    break

  case 'ZRANGE':
  case 'ZREVRANGE':
  case 'ZRANGEBYLEX':
  case 'ZREVRANGEBYLEX':
  case 'ZRANGEBYSCORE':
  case 'ZREVRANGEBYSCORE':
    // REVIEW: by lex has no WITHSCORES option
    var withScore = args[3] && (args[3].toUpperCase() === 'WITHSCORES')
    columns = [ 'member' ]
    if (withScore) {
      columns.push('score')
    }
    for (var i = 0; i < result.length; i++) {
      var row = [ result[i] ]
      if (withScore) {
        i++
        row.push(result[i])
      }
      rows.push(row)
    }
    break
  }

  var results = {}
  results["table"] = {
    columns : columns.map(function (col) { return { text: col, type: 'text' } }),
    rows : rows,
    type : "table"
  }
  return results
}

// TODO: Runs a query to support templates.
function doTemplateQuery(requestId, queryArgs, db, res, next)
{
  if (queryArgs.err == null) {
    var err = new Error('Unsupport templates query')
    queryError(requestId, err, next)
  } else {
    next(queryArgs.err)
  }
}

function logRequest(body, type)
{
  if (serverConfig.logRequests)
  {
    console.log("REQUEST: " + type + ":\n" + JSON.stringify(body,null,2))
  }
}

function logQuery(command, args)
{
  if (serverConfig.logQueries) {
    console.log("Command:", command)
    if (args != null) {
      console.log("Args:")
      console.log(JSON.stringify(args,null,2))
    }
  }
}

function logTiming(query, elapsedTimeMs)
{
  if (serverConfig.logTimings) {
    console.log("Request Query: " + JSON.stringify(query) + " - Returned in " + elapsedTimeMs.toFixed(2) + "ms")
  }
}
