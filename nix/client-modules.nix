{
  stdenv,
  bun,
}:
stdenv.mkDerivation {
  pname = "client-modules";
  version = "main";

  src = ../client;

  outputHash = "sha256-t8PJFo+3XGkzmMNbw9Rf9cS5Ob5YtI8ucX3ay+u9a3M=";
  outputHashAlgo = "sha256";
  outputHashMode = "recursive";

  nativeBuildInputs = [bun];

  dontCheck = true;
  dontConfigure = true;
  dontFixup = true;
  dontPatchShebangs = true;

  buildPhase = "bun install --no-cache --no-progress --frozen-lockfile";
  installPhase = ''
    mkdir -p $out

    # Do not copy .cache or .bin
    cp -R ./node_modules/* $out
    cp -R ./node_modules/.bin $out
    ls -la $out
  '';
}
