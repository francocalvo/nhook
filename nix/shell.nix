{ pkgs, uvSet }:

let
  editableOverlay = uvSet.workspace.mkEditablePyprojectOverlay {
    root = "$REPO_ROOT";
    members = [ "notion-hook" ];
  };
  editablePythonSet = uvSet.pythonSet.overrideScope editableOverlay;
  localVenv = editablePythonSet.mkVirtualEnv "notion-hook-env" {
    inherit (uvSet.workspace.deps.all) notion-hook;
  };
in
pkgs.mkShell {
  packages = [
    localVenv
    pkgs.uv
    pkgs.git
    pkgs.ruff
    pkgs.pyright
    pkgs.ngrok
  ]
  ++ uvSet.dependencies;

  shellHook = ''
    unset PYTHONPATH
    export UV_NO_SYNC=1
    export UV_PYTHON_DOWNLOADS=never
    export REPO_ROOT=$(git rev-parse --show-toplevel)
    if [ -f .env ]; then
      set -a && source .env && set +a
    fi
  '';
}
