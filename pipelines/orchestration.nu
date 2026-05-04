export def start_orchestrator [
  --host="127.0.0.1:7000", 
  --queue=10000, 
  --refresh=1
] {
  $env.RUST_LOG = "info"
  ^orchestrator $host $queue $refresh
}
export def start_worker_monitor [
  --host="127.0.0.1:7010"
] {
  let responses = ^consumer $host --raw
  | from msgpack --objects
  | flatten
  return $responses
}
