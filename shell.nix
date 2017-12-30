with import <nixpkgs> { };
stdenv.mkDerivation {
  name = "sg-car";
  buildInputs = with pkgs; [ rust-nightly gpgme ];
  shellHook = ''
    export CARGO_INCREMENTAL=1
    export GPGME_CONFIG="${gpgme.dev}/bin/gpgme-config"
  '';
}
