export def --wrapped start_backup_worker [
  --host_consumer="127.0.0.1:7000",
  --host_producer="127.0.0.1:7010",
  ...oci_copy_args
] {
  ^consumer $host_consumer --raw
  | from msgpack --objects
  | each { |payload|
    let result = (oci os object copy
      --source-object-name $payload.name
      --source-object-if-match-e-tag $payload.etag
      ...$oci_copy_args
    | complete)

    let report = if $result.exit_code == 0 {
      {status: "ok", stdout: ($result.stdout | from json)}
    } else {
      {
        status: "error",
        code: $result.exit_code,
        stdout: $result.stdout,
        stderr: $result.stderr
      }
    }

    {name: $payload.name, report: $report} | to msgpack
                                           | ^producer $host_producer --msgpack
  }
}

export def --wrapped start_backup_swarm [
  num_workers
  --host_consumer="127.0.0.1:7000",
  --host_producer="127.0.0.1:7010",
  --log_dir="/tmp",
  ...oci_copy_args
] {
  let worker_ids = 1..$num_workers | each {|_|
    job spawn {
      (start_backup_worker
        --host_consumer $host_consumer
        --host_producer $host_producer
        ...$oci_copy_args)
    }
  }
}

