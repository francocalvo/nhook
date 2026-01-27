{
  description = "Notion webhook handler";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/24.11";
    flake-utils.url = "github:numtide/flake-utils";

    pyproject-nix = {
      url = "github:pyproject-nix/pyproject.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    uv2nix = {
      url = "github:pyproject-nix/uv2nix";
      inputs.pyproject-nix.follows = "pyproject-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    pyproject-build-systems = {
      url = "github:pyproject-nix/build-system-pkgs";
      inputs.pyproject-nix.follows = "pyproject-nix";
      inputs.uv2nix.follows = "uv2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      uv2nix,
      pyproject-nix,
      pyproject-build-systems,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };

        # Load Python/UV configuration
        uvSet = import ./nix/uv.nix {
          inherit
            pkgs
            uv2nix
            pyproject-nix
            pyproject-build-systems
            ;
        };

        # Helper from pyproject.nix for building apps
        mkApp = pkgs.callPackage pyproject-nix.build.util { };

        # The application package
        app = mkApp.mkApplication {
          venv = uvSet.pythonSet.mkVirtualEnv "notion-hook-env" {
            inherit (uvSet.workspace.deps.all) notion-hook;
          };
          package = uvSet.pythonSet.notion-hook;
        };

        # Docker image
        dockerImage = import ./nix/docker.nix {
          inherit pkgs pyproject-nix uvSet;
        };

        # Development shell
        devShell = import ./nix/shell.nix {
          inherit pkgs uvSet;
        };

      in
      {
        packages = {
          default = app;
          dockerImage = dockerImage;
        };

        devShells = {
          default = devShell;
        };
      }
    );
}
