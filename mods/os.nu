export def paginate_os_objects [bucket_name limit=1024, --verbose (-v)] {
  mut pg = ""
  mut pn = 1
  mut os = []
  loop {
    if $verbose {
      print -n $"Working on ($limit)-element page number: ($pn)\r"
    }
    let src = (
      oci os object list --bucket-name $bucket_name --start $pg --limit $limit
      | from json
    )

    if $src has data {
      $os = ($os | append $src.data)
    } else {
      make error {
        msg: "Unexpected output from 'os object list': output has no field called 'data'"
      }
    }

    if $src has next-start-with {
      $pg = $src.next-start-with
    } else {
      if $verbose {
        print ""
        print "done!"
      }
      break
    }

    $pn = $pn + 1
  }

  return $os
}
