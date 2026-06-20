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
    
    for workers in [1, 3, 5, 10]:
        try:
            jps, p50, p95, p99 = run_benchmark(workers, args.type)
            print(f"{workers:>8} | {jps:>10.1f} | {p50:>7.0f} | {p95:>7.0f} | {p99:>7.0f}")
        except Exception as e:
            print(f"{workers:>8} | Error: {e}")
            break
