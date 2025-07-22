{
  inputs.parts.url = "github:hercules-ci/flake-parts";
  inputs.nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";

  outputs = inp:
    inp.parts.lib.mkFlake {inputs = inp;} {
      systems = ["x86_64-linux"];
      perSystem = {
        pkgs,
        config,
        ...
      }: {
        packages.client-modules = pkgs.callPackage ./nix/client-modules.nix { };
        packages.client = pkgs.callPackage ./nix/client.nix {
          inherit (config.packages) client-modules;
        };
        packages.server = pkgs.callPackage ./nix/server.nix { };
      };
    };
}
