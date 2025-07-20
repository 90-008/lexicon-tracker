{
  inputs.parts.url = "github:hercules-ci/flake-parts";
  inputs.nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
  inputs.systems.url = "github:nix-systems/x86_64-linux";
  inputs.naked-shell.url = "github:90-008/mk-naked-shell";
  inputs.nci.url = "github:yusdacra/nix-cargo-integration";
  inputs.nci.inputs.nixpkgs.follows = "nixpkgs";

  outputs = inp:
    inp.parts.lib.mkFlake {inputs = inp;} {
      systems = import inp.systems;
      imports = [
        inp.naked-shell.flakeModule
        inp.nci.flakeModule
      ];
      perSystem = {
        config,
        system,
        ...
      }: let
        pkgs = inp.nixpkgs.legacyPackages.${system};
        packageJson = builtins.fromJSON (builtins.readFile ./client/package.json);
      in {
        nci.projects."nsid-tracker" = {
          export = false;
          path = ./server;
        };
        packages.client-modules = pkgs.stdenv.mkDerivation {
          pname = "${packageJson.name}-modules";
          version = packageJson.version;

          src = ./client;

          outputHash = "sha256-TzTafbNTng/mMyf0yR9Rc6XS9/zzipwmK9SUWm2XxeY=";
          outputHashAlgo = "sha256";
          outputHashMode = "recursive";

          nativeBuildInputs = with pkgs; [bun];

          dontConfigure = true;

          buildPhase = "bun install --no-cache --no-progress --frozen-lockfile";
          installPhase = ''
            mkdir -p $out

            # Do not copy .cache or .bin
            cp -R ./node_modules/* $out
            cp -R ./node_modules/.bin $out
            ls -la $out
          '';
          dontFixup = true;
          dontPatchShebangs = true;
        };
        packages.client = pkgs.stdenv.mkDerivation {
          pname = packageJson.name;
          version = packageJson.version;

          src = ./client;

          nativeBuildInputs = [pkgs.makeBinaryWrapper];
          buildInputs = [pkgs.bun];

          # dontConfigure = true;
          configurePhase = ''
            runHook preConfigure
            cp -R --no-preserve=ownership ${config.packages.client-modules} node_modules
            find node_modules -type d -exec chmod 755 {} \;
            substituteInPlace node_modules/.bin/vite \
              --replace-fail "/usr/bin/env node" "${pkgs.bun}/bin/bun"
            runHook postConfigure
          '';
          buildPhase = ''
            runHook preBuild
            bun --prefer-offline run --bun build
            runHook postBuild
          '';
          installPhase = ''
            runHook preInstall
            cp -R ./build/* $out
            runHook postInstall
          '';
        };
        packages.server = config.nci.outputs."server".packages.release;
      };
    };
}
