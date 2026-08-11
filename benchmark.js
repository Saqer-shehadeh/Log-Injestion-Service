const autocannon = require('autocannon');

async function runBenchmark() {
  console.log(
    '🚀 Starting Benchmark Test against http://localhost:8080 ...\n'
  );

  // =========================
  // Benchmark Configuration
  // =========================

  const LOGS_PER_REQUEST = 50;
  const TEST_DURATION = 60;
  const CONNECTIONS = 1;
  const PIPELINING = 1;

  const batchPayload = JSON.stringify({
    logs: Array.from({ length: LOGS_PER_REQUEST }, (_, i) => ({
      timestamp: new Date().toISOString(),
      level: i % 2 === 0 ? 'info' : 'error',
      service: i % 3 === 0 ? 'checkout' : 'auth',
      message: `User payment status updated event #${i}`,
      attributes: {
        user_id: `usr_${i}`,
        region: 'eu-west',
        status_code: 200 + (i % 5),
      },
    })),
  });

  const errorDetails = [];

  // =========================
  // Start Autocannon
  // =========================

  const instance = autocannon({
    url: 'http://localhost:8080/logs',

    connections: CONNECTIONS,
    pipelining: PIPELINING,

    duration: TEST_DURATION,

    method: 'POST',

    headers: {
      'content-type': 'application/json',
    },

    body: batchPayload,
  });

  instance.on('error', (err) => {
    errorDetails.push({
      message: err.message,
      code: err.code,
      stack: err.stack,
    });
  });

  const result = await instance;

  // =========================
  // Calculate Results
  // =========================

  const totalRequests = result.requests.total;

  const requestsPerSecond =
    totalRequests / TEST_DURATION;

  /*
   * Each successful benchmark request contains
   * exactly LOGS_PER_REQUEST logs.
   *
   * This gives the theoretical submitted log rate.
   */
  const logsPerSecond =
    requestsPerSecond * LOGS_PER_REQUEST;

  // =========================
  // Print Results
  // =========================

  console.log('====================================================');
  console.log('🔥 BENCHMARK RESULTS');
  console.log('====================================================');

  console.log(`Connections        : ${CONNECTIONS}`);
  console.log(`Pipelining         : ${PIPELINING}`);
  console.log(`Duration           : ${TEST_DURATION}s`);
  console.log(`Logs/request       : ${LOGS_PER_REQUEST}`);

  console.log('');

  console.log(
    `HTTP Requests/sec  : ${requestsPerSecond.toFixed(2)}`
  );

  console.log(
    `Logs Processed/sec : ${logsPerSecond.toFixed(2)}`
  );

  console.log('');

  console.log(
    `Latency Average    : ${result.latency.average} ms`
  );

  console.log(
    `Latency p50        : ${result.latency.p50} ms`
  );

  console.log(
    `Latency p95        : ${result.latency.p95 ?? 'N/A'} ms`
  );

  console.log(
    `Latency p99        : ${result.latency.p99} ms`
  );

  console.log('');

  console.log(`Total Requests     : ${totalRequests}`);
  console.log(`Total Errors       : ${result.errors}`);
  console.log(`Timeouts           : ${result.timeouts}`);
  console.log(`4xx/5xx Responses  : ${result.non2xx}`);

  console.log('====================================================');

  console.log('\nError Details:');

  console.log(
    JSON.stringify(errorDetails.slice(0, 20), null, 2)
  );

  // =========================
  // Pass / Fail
  // =========================

  if (
    logsPerSecond >= 15000 &&
    result.errors === 0 &&
    result.timeouts === 0 &&
    result.non2xx === 0
  ) {
    console.log('\n✅ PASSED: 15k+ logs/sec sustained');
  } else {
    console.log('\n❌ FAILED: Need more tuning');
  }
}

runBenchmark().catch((err) => {
  console.error('Benchmark crashed:', err);
  process.exit(1);
});