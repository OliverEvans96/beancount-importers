{
  inputs = {
    nixpkgs-unstable.url = "nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };
  outputs = { self, nixpkgs-unstable, flake-utils }:
    let
      pkgs = nixpkgs-unstable.legacyPackages.x86_64-linux;
      # Inpsired by https://github.com/nix-community/poetry2nix/issues/218#issuecomment-981615612
      addDeps = super:
        builtins.mapAttrs (pkgName: deps:
          super."${pkgName}".overridePythonAttrs
          (old: { buildInputs = (old.buildInputs or [ ]) ++ deps; }));
      addNativeDeps = super:
        builtins.mapAttrs (pkgName: deps:
          super."${pkgName}".overridePythonAttrs (old: {
            nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ deps;
          }));
      mergeSets = builtins.foldl' pkgs.lib.mergeAttrs { };
      mkOverrides = args: super:
        mergeSets (builtins.attrValues (builtins.mapAttrs (attr: attrArgs:
          (builtins.mapAttrs (pkgName: deps:
            super."${pkgName}".overridePythonAttrs
            (old: { "${attr}" = (old."${attr}" or [ ]) ++ deps; })) attrArgs))
          args));
      myShell = pkgs.poetry2nix.mkPoetryEnv {
        projectDir = ./.;
        extraPackages = (ps: with ps; [ python-lsp-server ]);
        python = pkgs.python38;
        editablePackageSources = { beancount-importers = ./.; };
        overrides = self:
          mkOverrides {
            buildInputs = with self; {
              rsa = [ poetry ];
              soupsieve = [ hatchling ];
              tomli = [ flit-core ];
              pyparsing = [ flit-core ];
            };
            nativeBuildInputs = with pkgs; { lxml = [ libxml2 libxslt ]; };
          };
      };
    in flake-utils.lib.eachDefaultSystem (system: {
      defaultPackage = pkgs.hello;
      devShell = myShell.env;
    });
}
