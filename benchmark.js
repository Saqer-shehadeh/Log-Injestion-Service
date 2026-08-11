const autocannon = require('autocannon');

async function runBenchmark() {
  console.log('🚀 Starting Benchmark Test against http://localhost:8080 ...\n');

  const LOGS_PER_REQUEST = 50;
  const TEST_DURATION = 600;

  const batchPayload = JSON.stringify({
    logs: Array.from({ length: LOGS_PER_REQUEST }, (_, i) => ({
      timestamp: new Date().toISOString(),
      level: i % 2 === 0 ? 'info' : 'error',
      service: i % 3 === 0 ? 'checkout' : 'auth',
      message: `User payment status updated event #${i}`,
      attributes: {
        user_id: `usr_${i}`,
        region: 'eu-west',
        status_code: 200 + (i % 5)
      }
    }))
  });

  const errorDetails = [];

  const instance = autocannon({
    url: 'http://localhost:8080/logs',

    connections: 40,
    pipelining: 1,

    duration: TEST_DURATION,

    method: 'POST',

    headers: {
      'content-type': 'application/json'
    },

    body: batchPayload
  });


  instance.on('error', (err) => {
    errorDetails.push({
      message: err.message,
      code: err.code,
      stack: err.stack
    });
  });


  const result = await instance;


  // Correct calculation
  const requestsPerSecond = result.requests.total / TEST_DURATION;
  const logsPerSecond = requestsPerSecond * LOGS_PER_REQUEST;


  console.log('====================================================');
  console.log('🔥 BENCHMARK RESULTS');
  console.log('====================================================');

  console.log(`Connections        : 1`);
  console.log(`Pipelining         : 1`);
  console.log(`Duration           : ${TEST_DURATION}s`);
  console.log(`Logs/request       : ${LOGS_PER_REQUEST}`);

  console.log('');

  console.log(`HTTP Requests/sec  : ${requestsPerSecond.toFixed(2)}`);
  console.log(`Logs Processed/sec : ${logsPerSecond.toFixed(2)}`);

  console.log('');

  console.log(`Latency Average    : ${result.latency.average} ms`);
  console.log(`Latency p50        : ${result.latency.p50} ms`);
  console.log(`Latency p99        : ${result.latency.p99} ms`);

  console.log('');

  console.log(`Total Requests     : ${result.requests.total}`);
  console.log(`Total Errors       : ${result.errors}`);
  console.log(`Timeouts           : ${result.timeouts}`);
  console.log(`4xx/5xx Responses  : ${result.non2xx}`);

  console.log('====================================================');


  console.log('\nError Details:');
  console.log(JSON.stringify(errorDetails.slice(0, 20), null, 2));


  if (
    logsPerSecond >= 15000 &&
    result.errors === 0 &&
    result.timeouts === 0 &&
    result.non2xx === 0
  ) {
    console.log('\n✅ PASSED');
  } else {
    console.log('\n❌ FAILED');
  }
}


runBenchmark();