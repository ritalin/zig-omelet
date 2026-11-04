{
    description = "zig-omelet dev env";

    inputs = {
        nixpkgs.url = "github:NixOS/nixpkgs";
        flake-utils.url = "github:numtide/flake-utils";
    };

    outputs = { self, nixpkgs, flake-utils }: flake-utils.lib.eachDefaultSystem(
        system:
        let
            pkgs = nixpkgs.legacyPackages.${system};
            nng-static = pkgs.nng.overrideAttrs (old: {
              cmakeFlags =
                (old.cmakeFlags or [])
                ++ [ "-DBUILD_SHARED_LIBS=OFF" ];
            });
        in {
            packages.nng = nng-static;
            devShells.default = pkgs.mkShell {
              buildInputs = [
                pkgs.zsh
                pkgs.starship
                pkgs.bintools
                pkgs.catch2_3
                nng-static
              ];
              shellHook = ''
                export NNG_PREFIX=${pkgs.nng}
                export CATCH2_PREFIX=${pkgs.catch2_3}
                eval "$(starship init bash)"
              '';
            };
        }
    );
}
