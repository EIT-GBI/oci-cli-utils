#!/usr/bin/env nu
use ../mods/jobs.nu spawn
use ../mods/jobs.nu await 
use ./orchestration.nu start_orchestrator

const TAGS = [in-pipe out-pipe]

def main [
  --refresh: int = 60
  --queue: int   = 1024
  --host_in: string  = "127.0.0.1:7000"
  --host_out: string = "127.0.0.1:7010"
] {
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

  try {
    await ...$TAGS
  } catch {
    print "\nInterrupted — killing workers..."
    job list | where description in $TAGS | each {|j| job kill $j.id }
  }
}
