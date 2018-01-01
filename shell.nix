with import <nixpkgs> { };
stdenv.mkDerivation {
  name = "rackup";
  buildInputs = with pkgs; [ rust-nightly pkgconfig libsodium zstd xz ];
  shellHook = ''
    export CARGO_INCREMENTAL=1
    export GPGME_CONFIG="${gpgme.dev}/bin/gpgme-config"
  '';
}
