{
  pkgs,
  uv2nix,
  pyproject-nix,
  pyproject-build-systems,
}:

let
  # Choose your Python version
  python = pkgs.python312;

  dependencies = [ python ];

  # Load your local Python workspace
  workspace = uv2nix.lib.workspace.loadWorkspace { workspaceRoot = ../.; };

  # Create an overlay from your Python project
  baseOverlay = workspace.mkPyprojectOverlay { sourcePreference = "wheel"; };

  # Build system overrides for hatchling and editables
  buildSystemOverrides = {
    notion-hook = {
      hatchling = [ ];
      editables = [ ];
    };
  };

  applyBuildSystemOverrides =
    final: prev:
    let
      inherit (final) resolveBuildSystem;
      inherit (builtins) mapAttrs;
    in
    mapAttrs (
      name: spec:
      prev.${name}.overrideAttrs (old: {
        nativeBuildInputs = old.nativeBuildInputs ++ resolveBuildSystem spec;
      })
    ) buildSystemOverrides;

  # Fix for calver package with invalid project.license configuration
  calverFix =
    final: prev:
    if prev ? calver then
      {
        calver = prev.calver.overrideAttrs (old: {
          postPatch = (old.postPatch or "") + ''
            # Fix invalid project.license configuration in pyproject.toml
            # Convert license = "Apache-2.0" to license = { text = "Apache-2.0" }
            sed -i 's/^license = "Apache-2.0"$/license = { text = "Apache-2.0" }/' pyproject.toml
            # Also handle any other license formats that might exist
            sed -i 's/^license = \([^{].*\)$/license = { text = \1 }/' pyproject.toml
          '';
        });
      }
    else
      { };

  pyprojectOverrides =
    final: prev:
    let
      overriddenPackages = applyBuildSystemOverrides final prev;
      calverFixed = calverFix final prev;
    in
    prev // overriddenPackages // calverFixed;

  # Combine overlays
  pythonSet =
    (pkgs.callPackage pyproject-nix.build.packages (
      {
        inherit python;
      }
      // pkgs.lib.optionalAttrs pkgs.stdenv.isDarwin {
        stdenv = pkgs.stdenv.override {
          targetPlatform = pkgs.stdenv.targetPlatform // {
            darwinSdkVersion = "15.1";
          };
        };
      }
    )).overrideScope
      (
        pkgs.lib.composeManyExtensions [
          pyproject-build-systems.overlays.default
          baseOverlay
          pyprojectOverrides
        ]
      );

in
{
  inherit
    pythonSet
    workspace
    dependencies
    python
    ;
}
