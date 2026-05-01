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

export def start_backup_worker [
  src_name,
  dst_name,
  --host_consumer="127.0.0.1:7000",
  --host_producer="127.0.0.1:7010"
] {
  let qpiped = $env.QPIPE_DIR
  let consumer_path = ($qpiped | path join nu_consumer | path expand)
  let producer_path = ($qpiped | path join producer | path expand)

  ^$consumer_path $host_consumer --jsonl 
  | from json --objects
  | each { |payload|
      print $"Copying: '($payload.name)'"
      (oci os object copy
        --bucket-name $src_name
        --source-object-name $payload.name
        --source-object-if-match-e-tag $payload.etag
        --destination-bucket $dst_name
      | from json)
      {type: 1, name: $payload.name} | to json -r
                                     | str join (char newline)
                                     | ^$producer_path $host_producer 
  }
}

export def start_backup_swarm [
  src_name,
  dst_name,
  num_workers
  --host_consumer="127.0.0.1:7000",
  --host_producer="127.0.0.1:7010",
  --log_dir="/tmp"
] {
  let worker_ids = 1..$num_workers | each {|_|
    job spawn {
      (start_backup_worker $src_name $dst_name 
        --host_consumer $host_consumer
        --host_producer $host_producer
        out+err> ($log_dir | path join $"worker-(random uuid).log"))
    }
  }
}

export def send_to_backup_workers [
  --host="127.0.0.1:7000"
] {
  let qpiped = $env.QPIPE_DIR
  let full_path = ($qpiped | path join producer | path expand)
  $in | shuffle
      | each { |row| $row | to json -r } 
      | str join (char newline) 
      | ^$full_path $host
}
