require('dotenv').config()
const express = require('express')
const pg = require('pg')
const TokenBucket = require('./tokenbucket')

const app = express()
// configs come from standard PostgreSQL env vars
// https://www.postgresql.org/docs/9.6/static/libpq-envars.html
const pool = new pg.Pool()

const queryHandler = (req, res, next) => {
  pool.query(req.sqlQuery).then((r) => {
    return res.json(r.rows || [])
  }).catch(next)
}

app.get('/', limitRequests(10, 50), (req, res) => {
  res.send('Welcome to EQ Works 😎')
})

app.get('/events/hourly', limitRequests(10, 50), (req, res, next) => {
  req.sqlQuery = `
    SELECT date, hour, events, name, lat, lon
    FROM public.hourly_events AS e
	LEFT OUTER JOIN public.poi AS p ON e.poi_id = p.poi_id
    ORDER BY date, hour
    LIMIT 168;
  `
  return next()
}, queryHandler)

app.get('/events/daily', limitRequests(10, 50), (req, res, next) => {
  req.sqlQuery = `
    SELECT date, SUM(events) AS events, name, lat, lon, p.poi_id
    FROM public.hourly_events AS e
	LEFT OUTER JOIN public.poi AS p ON e.poi_id = p.poi_id
    GROUP BY date,name,lat,lon, p.poi_id
    ORDER BY date
    LIMIT 7;
  `
  return next()
}, queryHandler)

app.get('/stats/hourly', limitRequests(10, 50), (req, res, next) => {
  req.sqlQuery = `
    SELECT *
    FROM public.hourly_stats AS s
	LEFT OUTER JOIN public.poi AS p ON s.poi_id = p.poi_id
    ORDER BY date, hour
    LIMIT 168;
  `
  return next()
}, queryHandler)

app.get('/stats/daily', limitRequests(10, 50), (req, res, next) => {
  req.sqlQuery = `
    SELECT date,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(revenue) AS revenue,
		name,
		lat,
		lon
    FROM public.hourly_stats AS s
	LEFT OUTER JOIN public.poi AS p ON s.poi_id = p.poi_id
    GROUP BY date
    ORDER BY date
    LIMIT 7;
  `
  return next()
}, queryHandler)

app.get('/poi', limitRequests(10, 50), (req, res, next) => {
  req.sqlQuery = `
    SELECT *
    FROM public.poi;
  `
  return next()
}, queryHandler)

app.listen(process.env.PORT || 5555, (err) => {
  if (err) {
    console.error(err)
    process.exit(1)
  } else {
    console.log(`Running on ${process.env.PORT || 5555}`)
  }
})

// last resorts
process.on('uncaughtException', (err) => {
  console.log(`Caught exception: ${err}`)
  process.exit(1)
})
process.on('unhandledRejection', (reason, p) => {
  console.log('Unhandled Rejection at: Promise', p, 'reason:', reason)
  process.exit(1)
})

//Rate Limiter Implementation using Token Bucket Algorithm
function limitRequests(perSecond, maxBurst) {
  const buckets = new Map();

  // Return an Express middleware function
  return function limitRequestsMiddleware(req, res, next) {
      if (!buckets.has(req.ip)) {
          buckets.set(req.ip, new TokenBucket(maxBurst, perSecond));
      }

      const bucketForIP = buckets.get(req.ip);
      if (bucketForIP.take()) {
          next();
      } else {
          res.status(429).send('Client rate limit exceeded');
      }
  }
}

//limitRequests(1000/x allowed reqs for this interval, number of allowed Reqs)(0.05,3)
app.get('/test',
limitRequests(10, 50),
    (req, res) => {
        res.send('Welcome to EQ Works rate limited API 😎')
    }
);
