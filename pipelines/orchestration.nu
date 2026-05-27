use ../mods/jobs.nu spawn

def start_orchestrator [
  --host="127.0.0.1:7000", 
  --queue=10000, 
  --refresh=1
] {
  $env.RUST_LOG = "info"
  ^orchestrator $host $queue $refresh
}

export def start_pipeline [
  --host_in: string    = "127.0.0.1:7000"
  --host_out: string   = "127.0.0.1:7010"
  --queue: int         = 1024
  --refresh: int       = 1
  --tag_suffix: string = "pipe"
] {

  let TAGS = [$"in-($tag_suffix)" $"out-($tag_suffix)"]

  spawn $TAGS.0 {
    (start_orchestrator
      --refresh $refresh
      --host    $host_in
      --queue   $queue
    ) o+e>| lines | each {|x| print $"[ in] ($x)"}
  }

  spawn $TAGS.1 {
    (start_orchestrator
      --refresh $refresh
      --host    $host_out
      --queue   $queue
    ) o+e>| lines | each {|x| print $"[out] ($x)"}
  }

  ^healthcheck $host_in
  ^healthcheck $host_out
}

export def stop_orchestrator [
    --host = "127.0.0.1:7000"
    --hard
] {
    $env.RUST_LOG = "info"
    let flag = if $hard { "--shutdown" } else { "--drain" }
    ^orchestrator $flag $host
}

export def start_worker_monitor [
  --host="127.0.0.1:7010"
] {
  ^consumer $host --raw
  | from msgpack --objects
  | flatten
}

export def send_to_workers [
  --host_in: string  = "127.0.0.1:7000"
  --host_out: string = "127.0.0.1:7010"
] {
  $in | each { |row| {type: 1, payload: $row} | to msgpack }
      | bytes collect  # merge stream of binary values into one binary record
      | tee { ^producer $host_out --msgpack }
      | ^producer $host_in --msgpack
}

export def start_workers [ 
  num_workers
  worker: closure
  --host_consumer="127.0.0.1:7000",
  --host_producer="127.0.0.1:7010"
] {
  let worker_ids = 1..$num_workers | each {|_|
    job spawn {
      ^consumer $host_consumer --raw
      | from msgpack --objects
      | each { |elt| {type: 2, in: $elt.payload, out: (do $worker $elt.payload) } }
      | to msgpack
      | ^producer $host_producer --msgpack
    }
  }
  return $worker_ids
}
