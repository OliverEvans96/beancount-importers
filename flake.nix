{
  inputs = {
    nixpkgs-unstable.url = "nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };
  outputs = { self, fenix, nixpkgs, nixpkgs-unstable, flake-utils }:
      let
        pkgs = nixpkgs-unstable.legacyPackages.x86_64-linux;
        # Inpsired by https://github.com/nix-community/poetry2nix/issues/218#issuecomment-981615612
        addPythonDeps = super: builtins.mapAttrs (pkgName: pyDeps:
          super."${pkgName}".overridePythonAttrs (
            old: {
              buildInputs = (old.buildInputs or []) ++ pyDeps;
            }
          )
        );
        addNativeDeps = super: builtins.mapAttrs (pkgName: nativeDeps:
          super."${pkgName}".overridePythonAttrs (
            old: {
              nativeBuildInputs = (old.nativeBuildInputs or []) ++ nativeDeps;
            }
          )
        );
        mkOverrides = { pythonDeps, nativeDeps }: super:
          (addPythonDeps super pythonDeps) // (addNativeDeps super nativeDeps);
        myShell = pkgs.poetry2nix.mkPoetryEnv {
          projectDir = ./.;
          extraPackages = (ps: with ps; [ python-lsp-server ]);
          python = pkgs.python38;
          editablePackageSources = {
            my-app = ./.;
          };
          overrides = self: mkOverrides {
            pythonDeps = with self; {
              rsa = [ poetry ];
              soupsieve = [ hatchling ];
              tomli = [ flit-core ];
              pyparsing = [ flit-core ];
            };
            nativeDeps = with pkgs; {
              lxml = [ libxml2 libxslt ];
            };
          };
        };
      in
      flake-utils.lib.eachDefaultSystem
        (system:
          {
            defaultPackage = pkgs.hello;
            devShell = myShell.env;
          }
        );
}
