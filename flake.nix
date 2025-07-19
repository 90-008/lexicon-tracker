{
  inputs.parts.url = "github:hercules-ci/flake-parts";
  inputs.nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
  inputs.systems.url = "github:nix-systems/x86_64-linux";
  inputs.naked-shell.url = "github:90-008/mk-naked-shell";

  outputs = inp:
    inp.parts.lib.mkFlake {inputs = inp;} {
      systems = import inp.systems;
      imports = [
        inp.naked-shell.flakeModule
      ];
      perSystem = {
        config,
        system,
        ...
      }: let
        pkgs = inp.nixpkgs.legacyPackages.${system};
        packageJson = builtins.fromJSON (builtins.readFile ./package.json);
      in {
        packages.nsid-tracker-modules = pkgs.stdenv.mkDerivation {
          pname = "${packageJson.name}-modules";
          version = packageJson.version;

          src = ./.;

          outputHash = "";
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
        packages.nsid-tracker = pkgs.stdenv.mkDerivation {
          pname = packageJson.name;
          version = packageJson.version;

          src = ./.;

          nativeBuildInputs = [pkgs.makeBinaryWrapper pkgs.rsync];
          buildInputs = [pkgs.bun];

          PUBLIC_BASE_URL="http://localhost:5173";
          GUESTBOOK_BASE_URL="http://localhost:8080";

          # dontConfigure = true;
          configurePhase = ''
            runHook preConfigure
            cp -R --no-preserve=ownership ${config.packages.nsid-tracker-modules} node_modules
            find node_modules -type d -exec chmod 755 {} \;
            substituteInPlace node_modules/.bin/vite \
              --replace-fail "/usr/bin/env node" "${pkgs.nodejs-slim_latest}/bin/node"
            runHook postConfigure
          '';
          buildPhase = ''
            runHook preBuild
            bun --prefer-offline run --bun build
            runHook postBuild
          '';
          installPhase = ''
            runHook preInstall

            mkdir -p $out/bin
            cp -R ./build/* $out

            makeBinaryWrapper ${pkgs.bun}/bin/bun $out/bin/${packageJson.name} \
              --prefix PATH : ${pkgs.lib.makeBinPath [ pkgs.bun ]} \
              --add-flags "run --bun --no-install --cwd $out start"

            runHook postInstall
          '';
        };
        packages.default = config.packages.nsid-tracker;
      };
    };
}
