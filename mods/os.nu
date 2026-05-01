export def paginate_os_objects [
  bucket_name,
  limit=1024,
  --profile: string         # OCI config profile (e.g. SOURCE / TARGET)
  --region: string          # Override region (e.g. us-ashburn-1, eu-frankfurt-1)
  --namespace-name: string  # Object Storage namespace; auto-detected if omitted
  --verbose (-v)
] {
  # Build pass-through args only for flags the caller actually set,
  # so we don't clobber the CLI's own auto-detection / config defaults.
  mut extra = []
  if ($profile | is-not-empty)        { $extra = ($extra | append ["--profile" $profile]) }
  if ($region | is-not-empty)         { $extra = ($extra | append ["--region" $region]) }
  if ($namespace_name | is-not-empty) { $extra = ($extra | append ["--namespace-name" $namespace_name]) }

  mut pg = ""
  mut pn = 1
  mut os = []
  loop {
    if $verbose {
      print -n $"Working on ($limit)-element page number: ($pn)\r"
    }
    let src = (
      oci os object list --bucket-name $bucket_name --start $pg --limit $limit ...$extra
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
