{ pkgs, pyproject-nix, uvSet }:

let
  # Helper from pyproject.nix
  mkApp = pkgs.callPackage pyproject-nix.build.util { };

  # Create a full environment for Docker
  venv = uvSet.pythonSet.mkVirtualEnv "notion-hook-env" {
    inherit (uvSet.workspace.deps.default) notion-hook;
  };

  dockerApp = mkApp.mkApplication {
    venv = venv;
    package = uvSet.pythonSet.notion-hook;
  };

in
pkgs.dockerTools.buildImage {
  name = "notion-hook";
  tag = "latest";
  created = "now";

  copyToRoot = pkgs.buildEnv {
    name = "notion-hook-root";
    paths = [ dockerApp uvSet.python ] ++ uvSet.dependencies;
    pathsToLink = [ "/bin" "/lib" ];
  };

  extraCommands = ''
    mkdir -p data
  '';

  config = {
    Entrypoint = [ "${dockerApp}/bin/nhook" ];
    WorkingDir = "/data";
    Volumes = { "/data" = { }; };
    Env = [ "PYTHONUNBUFFERED=1" ];
  };
}
