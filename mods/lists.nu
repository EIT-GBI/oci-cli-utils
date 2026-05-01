export def only_in_src [src dst join_on=name] {
  let dst_marker = ($dst | insert __in_dst true)
  let only_in_src = (
    $src | join --left $dst_marker $join_on
         | where __in_dst == null
         | reject __in_dst
  )
  return $only_in_src
}
