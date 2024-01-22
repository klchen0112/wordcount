{ pkgs, ... }:

{
  # https://devenv.sh/basics/
  env.GREET = "devenv";

  # https://devenv.sh/scripts/
  scripts.hello.exec = "echo hello from $GREET";

  enterShell = ''
    git --version
  '';

  # https://devenv.sh/languages/
  # languages.nix.enable = true;
  languages.rust = {
    enable = true;
    # https://devenv.sh/reference/options/#languagesrustchannel
    channel = "nixpkgs";

    components = [ "rustc" "cargo" "clippy" "rustfmt" "rust-analyzer" ];
  };


  packages = with pkgs; [
    git
    libiconvReal
    sqlite
  ] ++ lib.optionals pkgs.stdenv.isDarwin (with pkgs.darwin.apple_sdk; [
    frameworks.Security
  ]);

  # https://devenv.sh/pre-commit-hooks/
  pre-commit.hooks = {
   rustfmt.enable = true;
   clippy.enable = true;
  };
}
