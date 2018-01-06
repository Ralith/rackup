with import <nixpkgs> { };
stdenv.mkDerivation {
  name = "rackup";
  buildInputs = with pkgs; [ rust-nightly pkgconfig libsodium zstd xz ];
  shellHook = ''
    export CARGO_INCREMENTAL=1
  '';
}
