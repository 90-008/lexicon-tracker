{
  stdenv,
  bun,
}:
stdenv.mkDerivation {
  pname = "client-modules";
  version = "main";

  src = ../client;

  outputHash = "sha256-TzTafbNTng/mMyf0yR9Rc6XS9/zzipwmK9SUWm2XxeY=";
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
