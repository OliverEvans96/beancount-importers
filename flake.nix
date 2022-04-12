{
  inputs = {
    nixpkgs-unstable.url = "nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };
  outputs = { self, nixpkgs-unstable, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs-unstable.legacyPackages.${system};
        # Inpsired by https://github.com/nix-community/poetry2nix/issues/218#issuecomment-981615612
        mergeSets = builtins.foldl' pkgs.lib.mergeAttrs { };
        mkOverrides = args: super:
          mergeSets (builtins.attrValues (builtins.mapAttrs (attr: attrArgs:
            (builtins.mapAttrs (pkgName: deps:
              super."${pkgName}".overridePythonAttrs
              (old: { "${attr}" = (old."${attr}" or [ ]) ++ deps; })) attrArgs))
            args));
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
        projectDir = ./.;
        makeApplication = python:
          pkgs.poetry2nix.mkPoetryApplication {
            inherit projectDir python overrides;
          };
        overlay = self: super: {
          beancount-importers = makeApplication self.python;
        };
        python = pkgs.python38;
        poetryEnv = pkgs.poetry2nix.mkPoetryEnv {
          inherit projectDir python overrides;
          extraPackages = (ps: with ps; [ python-lsp-server ]);
          editablePackageSources = { beancount-importers = ./.; };
        };
      in {
        inherit overlay;
        defaultPackage = makeApplication python;
        devShell = poetryEnv.env;
      });
}
