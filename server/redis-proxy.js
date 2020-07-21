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
  'TTL', 'EXISTS',
  'HGET', 'HMGET', 'HGETALL',
  'HEXISTS',
  'LINDEX', 'LRANGE',
  'LLEN',
  'SMEMBERS', 'SDIFF', 'SINTER', 'SUNION',
  'SISMEMBER',
  'ZRANGE', 'ZREVRANGE', 'ZRANGEBYLEX', 'ZREVRANGEBYLEX', 'ZRANGEBYSCORE', 'ZREVRANGEBYSCORE',
  'ZCARD', 'ZCOUNT', 'ZLEXCOUNT', 'ZRANK', 'ZREVRANK', 'ZSCORE',
]

var app = express()
app.use(bodyParser.json())

function getQueryPassword(req) {
  const auth = (req.headers.authorization || '').split(' ')
  if (auth[0] === 'Basic') {
    const b64auth = auth[1] || ''
    return Buffer.from(b64auth, 'base64').toString()
  }
  return null
}

// Called by test
app.all('/', function (req, res, next)
{
  logRequest(req.body, "/")
  setCORSHeaders(res)

  const opts = {}
  const pass = getQueryPassword(req)
  if (pass) {
    opts.password = pass
  }

  var client = redis.createClient(req.body.db.url, opts)
  var sent = false
  client.on('ready', function () {
    if (sent) {
      return
    }
    sent = true
    res.send({ status : "success",
               display_status : "Success",
               message : 'Redis Connection test OK' })
    client.quit()
    next()
  })
  client.on('error', function (err) {
    console.error('Redis Connection Error:', err)
    if (sent) {
      return
    }
    sent = true
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
  queryArgs = parseQuery(req, req.body.target, {})
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
      queryArgs = parseQuery(req, tg.target, substitutions)
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

function parseQuery(req, query, substitutions)
{
  var doc = { commands: [] }
  var queryErrors = []

  const opts = {}
  const pass = getQueryPassword(req)
  if (pass) {
    opts.password = pass
  }
  doc.connectOpts = opts

  var querys = query.split('\n')

  for (var i = 0; i < querys.length; i++) {
    var q = querys[i].trim()
    if (!q || q.startsWith('#')) {
      continue
    }

    var args = splitargs(q)
    if (args.length < 2) {
      queryErrors.push('line ' + (i + 1) + ': Query must have command and arguments')
      break
    }

    // Query is of the form <command> ...arguments
    // Check command first.
    var command = args.shift().toUpperCase()
    if (!COMMANDS.includes(command)) {
      queryErrors.push('line ' + (i + 1) + ': Unknown command ' + command + '')
      break
    }

    doc.commands.push({ command, args })
  }

  if (queryErrors.length > 0) {
    doc.err = new Error('Failed to parse query - ' + queryErrors.join(':'))
  }
  if (doc.commands.length <= 0) {
    doc.err = new Error('No command need to be executed')
  }

  return doc
}

// Run a query. Must return documents of the form
// { value : 0.34334, ts : <epoch time in seconds> }
function runQuery(requestId, queryId, body, queryArgs, res, next)
{
  var client = redis.createClient(body.db.url, queryArgs.connectOpts)
  client.on('ready', function () {
    logQuery(queryArgs.commands)
    var stopwatch = new Stopwatch(true)
    var done = 0
    var rowsList = []
    var lastErr = null
    var elapsedTimeMs
    var checkDone = function (err, k, cmd, result) {
      done++
      if (err) {
        lastErr = err
      }
      try {
        rowsList[k] = getTableResults(cmd.command, cmd.args, result, queryArgs.commands.length > 1 ? (k + 1) : null)
      } catch (err) {
        lastErr = err
      }

      if (done >= queryArgs.commands.length) {
        client.quit()
        elapsedTimeMs = stopwatch.stop()
        logTiming(queryArgs, elapsedTimeMs)
        if (lastErr) {
          queryError(requestId, lastErr, next)
          return
        }

        var sortedColumns = ['n', 'key', 'field', 'value', 'member', 'score', 'result']
        var columns = []
        for (var i = 0; i < rowsList.length; i++) {
          var rows = rowsList[i]
          if (!rows || rows.length <= 0) {
            continue
          }
          columns = _.union(columns, Object.keys(rows[0]))
        }
        var newColumns = []
        for (var i = 0; i < sortedColumns.length; i++) {
          if (columns.includes(sortedColumns[i])) {
            newColumns.push(sortedColumns[i])
          }
        }
        columns = newColumns

        var results = {}
        results["table"] = {
          type: "table",
          columns: columns.map(function (col) { return { text: col, type: 'text' } }),
        }
        var resultRows = []
        for (var i = 0; i < rowsList.length; i++) {
          var rows = rowsList[i]
          if (!rows || rows.length <= 0) {
            continue
          }
          resultRows = resultRows.concat(rows.map(function (row) {
            var line = []
            for (var j = 0; j < columns.length; j++) {
              if (columns[j] in row) {
                line.push(row[columns[j]])
              } else {
                line.push(null)
              }
            }
            return line
          }))
        }
        results['table'].rows = resultRows

        // Mark query as finished - will send back results when all queries finished
        queryFinished(requestId, queryId, results, res, next)
      }
    }
    for (var i = 0; i < queryArgs.commands.length; i++) {
      (function (k, cmd) {
        var cmd = queryArgs.commands[i]
        var args = cmd.args.concat(function (err, result) {
          checkDone(err, k, cmd, result)
        })
        client[cmd.command].apply(client, args)
      })(i, queryArgs.commands[i])
    }
  })
  client.on('error', function (err) {
    console.error('Redis Connection Error:', err)
    client.quit()
    queryError(requestId, err, next)
  })
}

function getTableResults(command, args, result, n = null)
{
  var rows = []
  switch (command) {
  case 'GET':
    rows.push({ key: args[0], value: result })
    break
  case 'MGET':
    for (var i = 0; i < args.length; i++) {
      rows.push({ key: args[i], value: result[i] })
    }
    break

  case 'HGET':
    rows.push({ key: args[0], field: args[1], value: result })
    break
  case 'HMGET':
    for (var i = 1; i < args.length; i++) {
      rows.push({ key: args[0], field: args[i], value: result[i - 1] })
    }
    break
  case 'HGETALL':
    for (var field in result) {
      rows.push({ key: args[0], field: field, value: result[field] })
    }
    break

  case 'LINDEX':
    rows.push({ key: args[0], value: result })
    break
  case 'LRANGE':
    for (var i = 0; i < result.length; i++) {
      rows.push({ key: args[0], value: result[i] })
    }
    break

  case 'SMEMBERS':
    for (var i = 0; i < result.length; i++) {
      rows.push({ key: args[0], member: result[i] })
    }
    break
  case 'SDIFF':
  case 'SINTER':
  case 'SUNION':
    for (var i = 0; i < result.length; i++) {
      rows.push({ member: result[i] })
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
    for (var i = 0; i < result.length; i++) {
      var row = { key: args[0], member: result[i] }
      if (withScore) {
        i++
        row.score = result[i]
      }
      rows.push(row)
    }
    break

  // only result
  case 'TTL':
  case 'EXISTS':
  case 'HEXISTS':
  case 'LLEN':
  case 'SISMEMBER':
  case 'ZCARD':
  case 'ZCOUNT':
  case 'ZLEXCOUNT':
  case 'ZRANK':
  case 'ZREVRANK':
  case 'ZSCORE':
    rows.push({ result: result })
    break
  }

  if (n !== null) {
    rows.forEach((row) => {
      row.n = n
    })
  }

  return rows
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

function logQuery(commands)
{
  if (serverConfig.logQueries) {
    console.log("Command:", commands.map(function (cmd) {
      return cmd.command + ' ' + cmd.args.join(' ')
    }).join('; '))
  }
}

function logTiming(query, elapsedTimeMs)
{
  if (serverConfig.logTimings) {
    console.log("Request Query: " + JSON.stringify(query) + " - Returned in " + elapsedTimeMs.toFixed(2) + "ms")
  }
}
