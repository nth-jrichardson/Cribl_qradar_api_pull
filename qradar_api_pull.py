#!/usr/bin/env python3
import argparse
import gzip
import requests
import json
import time
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# Disable InsecureRequestWarning for verify=False requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Global variables (easily changed)
resume_file = "resume_timestamp.txt"


def load_resume_time(start_time_str):
	"""
	Check for a local resume file. If found, parse its timestamp and compare
	it with start_time_str; whichever is later is our actual start time.
	Print a notice to stdout if we override the user's start time.
	"""
	try:
		if os.path.exists(resume_file):
			with open(resume_file, 'r') as f:
				resume_str = f.read().strip()
			resume_dt = datetime.strptime(resume_str, '%Y-%m-%d %H:%M:%S')
			user_dt = datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S')
			if resume_dt > user_dt:
				print(f"[INFO] Found resume file: overriding start-time from {start_time_str} to {resume_str}")
				return resume_dt
	except:
		pass
	# If no file or parse error, fall back to user input
	return datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S')


def save_resume_time(dt_obj):
	"""
	Save the timestamp to the resume file in '%Y-%m-%d %H:%M:%S' format.
	"""
	with open(resume_file, 'w') as f:
		f.write(dt_obj.strftime('%Y-%m-%d %H:%M:%S'))


def chunk_times(start_dt, end_time_str, max_minutes_per_chunk):
	"""
	Return a list of (chunk_start, chunk_end) tuples, each incremented by max_minutes.
	"""
	end_dt = datetime.strptime(end_time_str, '%Y-%m-%d %H:%M:%S')
	chunks = []
	current = start_dt
	while current < end_dt:
		chunk_end = current + timedelta(minutes=max_minutes_per_chunk)
		if chunk_end > end_dt:
			chunk_end = end_dt
		chunks.append((current, chunk_end))
		current = chunk_end
	return chunks


def send_events_to_cribl_http(url, events, token):
	"""
	Prepare newline-delimited JSON payload, gzip it, and send it to Cribl.
	Returns the response object.
	"""
	payload = "\n".join(json.dumps(event) for event in events).encode('utf-8')
	compressed_payload = gzip.compress(payload)
	headers = {
		'Content-Encoding': 'gzip',
		'Content-Type': 'application/json',
		'Authorization': token
	}
	response = requests.post(url, data=compressed_payload, headers=headers)
	print(f"[INFO] Cribl upload Status Code: {response.status_code}")
	print(f"[INFO] Cribl upload Response: {response.text}")
	return response


def upload_file(filename, events, cribl_http_endpoint, cribl_http_token, max_attempts=5):
	"""
	Try to upload the given events by reading from the file up to max_attempts times.
	Upon a successful upload (HTTP 200 with message "success"), delete the file.
	Returns True if the upload was successful, otherwise False.
	"""
	attempt = 0
	success = False
	while attempt < max_attempts and not success:
		response = send_events_to_cribl_http(cribl_http_endpoint, events, cribl_http_token)
		if response.status_code == 200:
			try:
				resp_json = response.json()
				if resp_json.get("message") == "success":
					success = True
					print(f"[INFO] Upload successful for {filename}. Deleting file.")
					os.remove(filename)
				else:
					print(f"[WARN] Upload response for {filename} did not indicate success: {resp_json}")
			except Exception as e:
				print(f"[ERROR] Exception parsing response for {filename}: {e}")
		else:
			print(f"[ERROR] Upload failed for {filename} with status code {response.status_code}")
		attempt += 1
		if not success and attempt < max_attempts:
			print(f"[INFO] Retrying upload for {filename} (attempt {attempt + 1}/{max_attempts})")
			time.sleep(1)
	if not success:
		print(f"[ERROR] Upload failed for {filename} after {max_attempts} attempts.")
	return success


def run_ariel_query(host_name, api_token, chunk_start, chunk_end):
	"""
	1. Start a search (POST).
	2. Poll the search (POST) until 'status' == 'COMPLETED'.
	3. Perform a single GET to retrieve results (no pagination).
	"""
	headers = {
		'SEC': api_token,
		'Version': '20.0',
		'Accept': 'application/json'
	}

	query_str = (
		f"SELECT DATEFORMAT(startTime, 'yyyy-MM-dd hh:mm:ss') AS TimeGenerated,sourceip,QIDNAME(qid) AS EventName,LOGSOURCENAME(logsourceid) AS 'LogSourceName',sourceport,destinationip,destinationport,username,UTF8(payload) AS Payload from events\r\n"
		f"START '{chunk_start.strftime('%Y-%m-%d %H:%M:%S')}' \r\n"
		f"STOP '{chunk_end.strftime('%Y-%m-%d %H:%M:%S')}'"
	)

	print(f"[INFO] Starting query: {chunk_start} -> {chunk_end}")
	search_url = f"https://{host_name}/api/ariel/searches?query_expression={requests.utils.quote(query_str)}"
	resp = requests.post(search_url, headers=headers, verify=False)
	resp.raise_for_status()
	search_info = resp.json()
	search_id = search_info.get("search_id")
	if not search_id:
		print(f"[WARN] No search_id returned for chunk: {chunk_start} -> {chunk_end}")
		return []

	# Poll until 'COMPLETED'
	status_url = f"https://{host_name}/api/ariel/searches/{search_id}"
	while True:
		time.sleep(5)  # Polling interval
		print(f"[INFO] Polling for chunk: {chunk_start} -> {chunk_end}")
		stat_resp = requests.post(status_url, headers=headers, verify=False)
		stat_resp.raise_for_status()
		stat_info = stat_resp.json()
		if stat_info.get("status") == "COMPLETED":
			print(f"[INFO] Query complete for chunk: {chunk_start} -> {chunk_end}")
			break

	# Single GET for results
	results_url = f"https://{host_name}/api/ariel/searches/{search_id}/results"
	results_resp = requests.get(results_url, headers=headers, verify=False)
	results_resp.raise_for_status()

	try:
		results_list = results_resp.json().get('events', [])
		print(f"[INFO] Saving results for chunk: {chunk_start} -> {chunk_end}")
	except:
		results_list = []
		print(f"[WARN] No results found for chunk: {chunk_start} -> {chunk_end}")

	if not isinstance(results_list, list):
		print(f"[WARN] Results are not a list for: {chunk_start} -> {chunk_end}")
		results_list = []

	print(f"[INFO] Completed query: {chunk_start} -> {chunk_end}, retrieved {len(results_list)} events")
	return results_list


def handle_chunk_results(all_events, new_events, max_events_per_file, cribl_http_endpoint, cribl_http_token):
	"""
	Extend all_events with new_events, and if we exceed max_events_per_file,
	write as many files as needed in slices of size max_events_per_file.
	Each file is uploaded individually to Cribl Cloud via send_events_to_cribl_http().
	After a successful upload (HTTP 200 with message "success"), delete the file.
	To avoid overwriting files created in the same second, we append an incremental counter to the filename.
	"""
	if not hasattr(handle_chunk_results, "file_counter"):
		handle_chunk_results.file_counter = 0

	all_events.extend(new_events)
	while len(all_events) >= max_events_per_file:
		file_events = all_events[:max_events_per_file]
		all_events = all_events[max_events_per_file:]
		filename = (
			f"events_{datetime.now().strftime('%Y%m%d%H%M%S')}"
			f"_{handle_chunk_results.file_counter}.json"
		)
		handle_chunk_results.file_counter += 1

		print(f"[INFO] Writing {len(file_events)} events to {filename}")
		with open(filename, 'w') as outfile:
			json.dump(file_events, outfile)

		upload_file(filename, file_events, cribl_http_endpoint, cribl_http_token)

	return all_events


def main(api_token, start_time_str, end_time_str, host_name, max_query_threads, max_events_per_file, max_minutes_per_chunk, cribl_http_endpoint, cribl_http_token):
	"""
	Chunks the time range, launches queries either in parallel (if max_query_threads > 1)
	or serially (if max_query_threads == 1).
	Accumulates results, writes them out in multiple files if needed whenever
	our list grows beyond max_events_per_file, uploads each file to Cribl Cloud,
	and tracks the most recent chunk end time to resume if needed.
	"""
	actual_start_dt = load_resume_time(start_time_str)
	time_chunks = chunk_times(actual_start_dt, end_time_str, max_minutes_per_chunk)
	all_events = []

	if max_query_threads == 1:
		# Single-threaded
		for c_start, c_end in time_chunks:
			chunk_events = run_ariel_query(host_name, api_token, c_start, c_end)
			all_events = handle_chunk_results(all_events, chunk_events, max_events_per_file, cribl_http_endpoint, cribl_http_token)
			save_resume_time(c_end)
	else:
		# Multi-threaded
		future_to_times = {}
		with ThreadPoolExecutor(max_workers=max_query_threads) as executor:
			for c_start, c_end in time_chunks:
				future = executor.submit(run_ariel_query, host_name, api_token, c_start, c_end)
				future_to_times[future] = (c_start, c_end)
			for future in as_completed(future_to_times):
				c_start, c_end = future_to_times[future]
				chunk_events = future.result()
				all_events = handle_chunk_results(all_events, chunk_events, max_events_per_file, cribl_http_endpoint, cribl_http_token)
				save_resume_time(c_end)

	# Flush remaining events
	if all_events:
		if not hasattr(handle_chunk_results, "file_counter"):
			handle_chunk_results.file_counter = 0
		filename = (
			f"events_{datetime.now().strftime('%Y%m%d%H%M%S')}"
			f"_{handle_chunk_results.file_counter}.json"
		)
		print(f"[INFO] Writing remaining {len(all_events)} events to {filename}")
		with open(filename, 'w') as outfile:
			json.dump(all_events, outfile)
		upload_file(filename, all_events, cribl_http_endpoint, cribl_http_token)

	print("[INFO] Script finished successfully")


def parse_args():
	parser = argparse.ArgumentParser(description="Run Ariel API queries and export JSON results with local resume and upload to Cribl Cloud.")
	parser.add_argument("--api-token", required=True, help="API token for QROC authentication")
	parser.add_argument("--start-time", required=True, help="Start datetime, e.g. '2024-01-01 00:00:00'")
	parser.add_argument("--end-time", required=True, help="End datetime, e.g. '2024-01-01 00:00:00'")
	parser.add_argument("--host-name", required=True, help="QRadar host name, e.g. 'myhostname.com'")
	parser.add_argument("--max-events-per-file", required=True, help="Maximum number of events per JSON file, e.g. 10000")
	parser.add_argument("--query-chunk-size", required=True, help="Number of minutes for each query chunk, e.g. 60")
	parser.add_argument("--num-threads", required=True, help="How many simultaneous query threads to run. 1 means single-threaded, higher means multi-threaded")
	parser.add_argument("--cribl-http-endpoint", required=True, help="Cribl HTTP Bulk API endpoint, e.g. 'https://default.yourinstance.cribl.cloud:10080/cribl/_bulk'")
	parser.add_argument("--cribl-http-token", required=True, help="Authentication token for Cribl HTTP bulk source")
	return parser.parse_args()


if __name__ == '__main__':
	args = parse_args()
	main(
		api_token=args.api_token,
		start_time_str=args.start_time,
		end_time_str=args.end_time,
		host_name=args.host_name,
		max_query_threads=int(args.num_threads),
		max_events_per_file=int(args.max_events_per_file),
		max_minutes_per_chunk=int(args.query_chunk_size),
		cribl_http_endpoint=args.cribl_http_endpoint,
		cribl_http_token=args.cribl_http_token
	)
	