const { performance } = require('perf_hooks');

async function benchmarkQuery() {

    while (true) {

        const start = performance.now();

        try {
            
            await fetch(
                "http://localhost:8080/logs/aggregate?since=2026-08-01T00:00:00Z&until=2026-08-02T00:00:00Z&bucket=1m"
            );

            console.log(
                "Aggregate:",
                (performance.now() - start).toFixed(2),
                "ms"
            );

        } catch (e) {
            console.error(e);
        }

        await new Promise(r => setTimeout(r,1000));
    }

}

benchmarkQuery();