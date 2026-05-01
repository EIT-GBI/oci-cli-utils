export def start_orchestrator [
  --host="127.0.0.1:7000", 
  --queue=10000, 
  --refresh=1
] {
  $env.RUST_LOG = "info"
  let qpiped = $env.QPIPE_DIR
  let full_path = ($qpiped | path join orchestrator | path expand)
  ^$full_path $host $queue $refresh
}

export def start_worker_monitor [
  --host="127.0.0.1:7010"
] {
  let qpiped = $env.QPIPE_DIR
  let consumer_path = ($qpiped | path join nu_consumer | path expand)
  let responses = ^$consumer_path $host --jsonl 
  | from json --objects
  | flatten
  return $responses
}
