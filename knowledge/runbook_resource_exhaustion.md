# Runbook: out of memory, CPU saturated or disk full on the VM

## Symptoms
- A container exited with code 137 or shows OOM-KILLED; or it crash-loops (restarts climbing).
- java.lang.OutOfMemoryError, "Container killed", "Killed" or "No space left on device" in logs.
- Data is late (a feed stale, stations behind) but every container is running: Spark logs say
  "Current batch is falling behind" or micro-batches take far longer than the trigger interval.
- Health checks "host memory below 90%", "host CPU not saturated", "disk below 85%" or
  "no container at its memory limit" fail.

## Context
- One Oracle Cloud VM runs the whole pipeline AND other apps (the Soundly music app, the
  Momentum Scanner, Pipeline Copilot itself). Any of them can starve the pipeline.
- The Spark streaming jobs (weather-stream, seismic-stream) are the biggest pipeline consumers.

## Likely causes
- The container hit ITS OWN memory limit: its memory was near its limit before the crash,
  while the host still had memory free. The job needs more memory (or has a leak).
- The whole VM ran out of memory: host memory above ~95% and little available, so the kernel
  killed a process. The cause is whichever container grew, often NOT the one that died.
- CPU saturation: 1-minute load far above the number of cores. Nothing crashes; work queues up
  and streaming falls behind. Look for the container with very high CPU %.
- Disk full: writes fail (lake files, Spark checkpoints, Kafka segments, Docker logs).

## How to check
- get_host_resources: host memory, load vs cores, disk, and every container's memory vs its
  limit and CPU %, largest first.
- get_resource_history (optionally container=<name>): memory climbing for hours = a leak or an
  app growing; a sudden spike = one heavy job; compare who grew before the crash.
- list_containers and get_container_logs: exit codes, OOM flag, the error before the crash.

## Fix
- Crashed pipeline container: restart it (approved fix), BUT if the host is still under pressure
  it will die again. The lasting fix is a human decision: stop or limit the container using the
  memory/CPU, give the job a memory limit or a smaller Spark heap (spark.driver.memory), or move
  an app off the VM.
- CPU saturation with nothing crashed: restarting the pipeline does not help. Stop or throttle
  the busy container (docker update --cpus), then the stream catches up from Kafka.
- Disk full: delete old Docker logs/images (docker system prune), old Kafka segments, or extend
  the volume. Then restart what failed.
- Data is not lost while a stream is down: Kafka retains messages and the stream resumes from
  its checkpoint.
