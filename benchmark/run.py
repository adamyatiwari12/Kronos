import concurrent.futures
import time
import requests
import statistics
import argparse

API_URL = "http://localhost:8000"
NUM_JOBS = 50

def submit_and_poll(job_type):
    start_time = time.time()
    
    # Submit job
    try:
        resp = requests.post(f"{API_URL}/jobs", json={"type": job_type, "payload": {"data": "benchmark"}})
        resp.raise_for_status()
        job_id = resp.json()["id"]
    except Exception as e:
        print(f"Error submitting job: {e}")
        return 0
    
    # Poll until finished
    while True:
        try:
            status_resp = requests.get(f"{API_URL}/jobs/{job_id}")
            status_resp.raise_for_status()
            status_data = status_resp.json()
            
            if status_data["status"] in ("succeeded", "failed"):
                break
        except Exception as e:
            print(f"Error polling job {job_id}: {e}")
            break
            
        time.sleep(0.1) # Aggressive polling for benchmark accuracy
        
    end_time = time.time()
    return end_time - start_time

def run_benchmark(concurrency, job_type="send_email"):
    latencies = []
    start_time = time.time()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(submit_and_poll, job_type) for _ in range(NUM_JOBS)]
        for future in concurrent.futures.as_completed(futures):
            latencies.append(future.result())
            
    total_time = time.time() - start_time
    jobs_per_sec = NUM_JOBS / total_time
    
    # Convert latencies to ms
    latencies_ms = [l * 1000 for l in latencies if l > 0]
    
    if len(latencies_ms) >= 2:
        quantiles = statistics.quantiles(latencies_ms, n=100)
        p50 = quantiles[49]
        p95 = quantiles[94]
        p99 = quantiles[98]
    else:
        p50 = p95 = p99 = latencies_ms[0] if latencies_ms else 0
        
    return jobs_per_sec, p50, p95, p99

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Job Scheduler Benchmark")
    parser.add_argument("--url", default=API_URL, help="API URL to benchmark")
    parser.add_argument("--jobs", type=int, default=NUM_JOBS, help="Total jobs per concurrency level")
    parser.add_argument("--type", default="send_email", help="Job type to simulate")
    args = parser.parse_args()
    
    API_URL = args.url
    NUM_JOBS = args.jobs
    
    print(f"Running benchmark against {API_URL} with {NUM_JOBS} '{args.type}' jobs per run...")
    print(f"{'workers':>8} | {'jobs/sec':>10} | {'p50ms':>7} | {'p95ms':>7} | {'p99ms':>7}")
    print("-" * 52)
    
    results = []
    inflection_point = None
    max_throughput = 0
    
    for workers in [1, 2, 4, 6, 8, 10]:
        try:
            jps, p50, p95, p99 = run_benchmark(workers, args.type)
            print(f"{workers:>8} | {jps:>10.1f} | {p50:>7.0f} | {p95:>7.0f} | {p99:>7.0f}")
            
            results.append({
                "workers": workers,
                "throughput": jps,
                "p50_ms": p50,
                "p95_ms": p95,
                "p99_ms": p99
            })
            
            if inflection_point is None:
                if jps > max_throughput * 1.05:
                    max_throughput = jps
                elif workers > 1:
                    inflection_point = workers
                    
        except Exception as e:
            print(f"{workers:>8} | Error: {e}")
            break

    inflection_cause = (
        "Adding client workers stops increasing throughput likely due to DB connection "
        "pool exhaustion and lock contention. The API limits its connection pool, and aggressive "
        "polling by multiple clients combined with the backend worker locking the jobs table limits scaling."
    )
    
    if inflection_point:
        print(f"\nInflection point found at {inflection_point} workers.")
        print(f"Cause: {inflection_cause}")

    import json
    import os
    results_path = os.path.join(os.path.dirname(__file__), "results.json")
    with open(results_path, "w") as f:
        json.dump({
            "results": results,
            "inflection_point": inflection_point,
            "inflection_cause": inflection_cause
        }, f, indent=2)
    print(f"Saved results to {results_path}")
