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
          ]))
    ];
  }
