{
  rustPlatform,
  cmake,
  ...
}:
rustPlatform.buildRustPackage {
  pname = "nsid-tracker-server";
  version = "main";

  src = ../server;

  nativeBuildInputs = [ cmake ];

  cargoLock = {
    lockFile = ../server/Cargo.lock;
    outputHashes = {
      "axum-tws-0.5.0" = "sha256-29LqmGmXRw4wcia4S9ht+WABi/TN3Ws68kMEm97NyjQ=";
    };
  };
}
