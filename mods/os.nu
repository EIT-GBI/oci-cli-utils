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
  let extra = $extra  # make immutable

  generate { |state = {pg: "", pn: 1, buf: [], done: false}|
    # Drain the buffer first
    if ($state.buf | is-not-empty) {

      # peel the first element off the page

      return {
        out: ($state.buf | first),
        next: ($state | update buf ($state.buf | skip 1))
      }

    } else if not $state.done {

      # Get the next page, and peel the first element

      if $verbose {
        print -n $"Working on ($limit)-element page number: ($state.pn)\r"
      }

      let src = (
        oci os object list --bucket-name $bucket_name --start $state.pg --limit $limit ...$extra
        | from json
      )

      let data = ($src.data | default [])
      let next_pg = ($src | get -o next-start-with | default "")

      return {
        out: ($data | first),
        next: {
          pg: $next_pg,
          pn: ($state.pn + 1),
          buf: ($data | skip 1),
          done: (not ($src has next-start-with))
        }
      }
    }
  }
}

# Mirror an OCI Object Storage bucket to a local directory. Re-running the
# command picks up new objects (existing files are skipped). Pass --verify to
# also checksum-check existing local files and re-download mismatches.
export def sync_bucket_local [
  bucket_name: string
  local_dir: string
  --profile: string
  --region: string
  --namespace-name: string
  --prefix: string = ""
  --parallel: int = 16
  --verify (-c)            # checksum-verify existing files; re-download on mismatch
  --verbose (-v)
] {
  mut extra = []
  if ($profile        | is-not-empty) { $extra = ($extra | append ["--profile" $profile]) }
  if ($region         | is-not-empty) { $extra = ($extra | append ["--region" $region]) }
  if ($namespace_name | is-not-empty) { $extra = ($extra | append ["--namespace-name" $namespace_name]) }

  mkdir $local_dir

  # ---- 1. Bulk download anything missing ----------------------------------
  if $verbose { print "==> Downloading new / missing objects..." }
  mut dl_args = [
    --bucket-name $bucket_name
    --download-dir $local_dir
    --no-overwrite
    --parallel-operations-count $parallel
  ]
  if ($prefix | is-not-empty) { $dl_args = ($dl_args | append ["--prefix" $prefix]) }
  oci os object bulk-download ...$dl_args ...$extra

  if not $verify {
    if $verbose { print "==> Done (no verification requested)." }
    return
  }

  # ---- 2. Verify checksums of existing files ------------------------------
  if $verbose { print "==> Listing remote objects for verification..." }
  let remote = (
    (paginate_os_objects $bucket_name
      --profile $profile
      --region $region
      --namespace-name $namespace_name)
  )
  # If you want only a sub-prefix verified:
  let remote = if ($prefix | is-not-empty) {
    $remote | where {|o| $o.name | str starts-with $prefix }
  } else { $remote }

  if $verbose { print $"==> Verifying ($remote | length) objects..." }

  let mismatches = (
    $remote | each {|obj|
      let path = ($local_dir | path join $obj.name)
      if not ($path | path exists) {
        # Shouldn't happen after bulk-download, but flag if it does.
        { name: $obj.name, reason: "missing-locally" }
      } else if ($obj.md5 | is-empty) {
        # Multipart upload — fall back to size comparison.
        let local_size = (ls $path | get 0.size | into int)
        if $local_size != ($obj.size | into int) {
          { name: $obj.name, reason: "size-mismatch" }
        }
      } else {
        let local_md5 = (open --raw $path | hash md5 --binary | encode base64)
        if $local_md5 != $obj.md5 {
          { name: $obj.name, reason: "md5-mismatch" }
        }
      }
    } | compact
  )

  if ($mismatches | is-empty) {
    if $verbose { print "==> All files verified OK." }
    return
  }

  print $"==> ($mismatches | length) file\(s\) failed verification; re-downloading:"
  $mismatches | print

  for m in $mismatches {
    let path = ($local_dir | path join $m.name)
    mkdir ($path | path dirname)
    (oci os object get
      --bucket-name $bucket_name
      --name $m.name
      --file $path
      ...$extra)
  }

  if $verbose { print "==> Re-download complete." }
}
