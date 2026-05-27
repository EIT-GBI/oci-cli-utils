#!/usr/bin/env nu
use ../mods/jobs.nu spawn
use ../mods/jobs.nu await 
use ./orchestration.nu start_orchestrator
use ./orchestration.nu stop_orchestrator
use ./orchestration.nu start_worker_monitor
use ./os_backup.nu start_backup_swarm

const TAGS = [in-pipe out-pipe worker-monitor]

def main [
  src: string,
  dst: string,
  num: int,
  --refresh: int = 60
  --queue: int   = 1024
  --profile: string = "DEFAULT"
  --region: string = "uk-london-1"
  --host_in: string  = "127.0.0.1:7000"
  --host_out: string = "127.0.0.1:7010"
] {
  print "Starting pipelines..."

  spawn $TAGS.0 {
    start_orchestrator --refresh $refresh --host $host_in --queue $queue o+e>| lines
    | each {|l| print $"[ in] ($l)"}
  }

  spawn $TAGS.1 {
    start_orchestrator --refresh $refresh --host $host_out --queue $queue o+e>| lines
    | each {|l| print $"[out] ($l)"}
  }


  ^healthcheck $host_in
  ^healthcheck $host_out

  print "... pipelines are healthy"

  print "Starting worker monitor ..."

  spawn $TAGS.2 {
    start_worker_monitor --host $host_out
  }

  print "... worker monitor has started"

  print "Starting workers ..."

  let ns = (oci os ns get --profile $profile | from json | get -o data)
  (start_backup_swarm $num 
    --bucket-name $src 
    --destination-bucket $dst 
    --namespace-name $ns
    --region $region
    --profile $profile)

  print "... workers have started"
  
  print "Sending work to workers (this will start paralle processing) ..."

  let os = (paginate_os_objects $src --namespace-name $ns --region $region --profile $profile)
            | tee { send_to_workers }

  print "... all work has been dispatched"

  let status = await $TAGS.2

  print "Stopping pipelines ..."
  stop_orchestrator $host_in
  stop_orchestrator $host_out
  print "... pipelines are stopped // can shut down now"

  try {
    await ...$TAGS
  } catch {
    print "\nInterrupted — killing workers..."
    job list | where description in $TAGS | each {|j| job kill $j.id }
  }
}
