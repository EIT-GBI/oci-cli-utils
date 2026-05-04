export def --env prepend_to_path [dir: path] {
    let expanded = ($dir | path expand)
    if not ($expanded | path exists) {
        error make { msg: $"directory does not exist: ($expanded)" }
    }
    let added = ($env | get -o USER_PATH_ADDITIONS | default [])
    $env.USER_PATH_ADDITIONS = ($added | append $expanded | uniq)
    $env.PATH = ($env.PATH | prepend $expanded | uniq)
}

export def --env remove_from_path [dir: path] {
    let expanded = ($dir | path expand)
    $env.PATH = ($env.PATH | where $it != $expanded)
    if 'USER_PATH_ADDITIONS' in $env {
        $env.USER_PATH_ADDITIONS = ($env.USER_PATH_ADDITIONS | where $it != $expanded)
    }
}

export def --env reset_path [] {
    if 'USER_PATH_ADDITIONS' not-in $env { return }
    let to_remove = $env.USER_PATH_ADDITIONS
    $env.PATH = ($env.PATH | where $it not-in $to_remove)
    hide-env USER_PATH_ADDITIONS
}
