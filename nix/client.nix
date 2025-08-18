{
  lib,
  stdenv,
  makeBinaryWrapper,
  bun,
  client-modules,
  PUBLIC_API_URL ? "http://localhost:3713",
}:
stdenv.mkDerivation {
  pname = "client";
  version = "main";

  src = ../client;

  inherit PUBLIC_API_URL;

  nativeBuildInputs = [makeBinaryWrapper];
  buildInputs = [bun];

  dontCheck = true;

  configurePhase = ''
    runHook preConfigure
    cp -R --no-preserve=ownership ${client-modules} node_modules
    find node_modules -type d -exec chmod 755 {} \;
    substituteInPlace node_modules/.bin/vite \
      --replace-fail "/usr/bin/env node" "${bun}/bin/bun --bun"
    runHook postConfigure
  '';
  buildPhase = ''
    runHook preBuild
    bun --prefer-offline run build
    runHook postBuild
  '';
  installPhase = ''
    runHook preInstall

    mkdir -p $out/bin
    cp -R ./build/* $out
    cp -R ./node_modules $out

    makeBinaryWrapper ${bun}/bin/bun $out/bin/website \
      --prefix PATH : ${lib.makeBinPath [ bun ]} \
      --add-flags "run --bun --no-install --cwd $out start"

    runHook postInstall
  '';
}
