{
rustPlatform,
...
}:
rustPlatform.buildRustPackage {
  pname = "nsid-tracker-server";
  version = "main";

  src = ../server;

  cargoLock.lockFile = ../server/Cargo.lock;
}
