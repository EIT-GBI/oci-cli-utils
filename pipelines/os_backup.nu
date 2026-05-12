export def start_backup_worker [
  src_name,
  dst_name,
  --host_consumer="127.0.0.1:7000",
  --host_producer="127.0.0.1:7010"
] {
  ^consumer $host_consumer --raw
  | from msgpack --objects
  | each { |payload|
      print $"Copying: '($payload.name)'"
      (oci os object copy
        --bucket-name $src_name
        --source-object-name $payload.name
        --source-object-if-match-e-tag $payload.etag
        --destination-bucket $dst_name
      | from json)

      {type: 1, name: $payload.name} | to msgpack
                                     | ^producer $host_producer --msgpack
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

