let
  pkgs =
    import (fetchTarball
      "https://github.com/NixOS/nixpkgs/archive/nixpkgs-unstable.tar.gz") {};
in
  pkgs.mkShell {
    packages = with pkgs; [
      protobuf
      grpc

      (haskell.packages.ghc927.ghcWithPackages
        (ghcPkgs:
          with ghcPkgs; [
            haskell-language-server
            cabal-install

            BNFC
            alex
            happy
          ]))
    ];
    shellHook = ''
      export PROTO_CPP_PLUGIN="$(which grpc_cpp_plugin)"
      export PROTOC_INCLUDE_DIR="$(dirname $(dirname $(which protoc)))/include"
    '';
  }
