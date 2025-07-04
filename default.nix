# nativelink-provisioner/default.nix
{ pkgs ? import <nixpkgs> {} }:

let
  # Read and parse Cargo.toml
  cargoToml = pkgs.lib.importTOML ./Cargo.toml;

  # Project details
  appName = cargoToml.package.name;
  appVersion = cargoToml.package.version;

  # 1. Build the Rust application
  # This uses rustPlatform.buildRustPackage to compile your crate.
  rustApp = pkgs.rustPlatform.buildRustPackage {
    pname = appName;
    version = appVersion;

    # Source code for the Rust application (current directory)
    src = ./.;

    # Specify the Cargo.lock file for reproducible dependency resolution
    cargoLock = {
      lockFile = ./Cargo.lock;
    };

    # Native build inputs are tools needed during the build process itself.
    nativeBuildInputs = with pkgs; [
      pkg-config
    ];

    buildInputs = with pkgs; [
      openssl
    ];

    # By default, buildRustPackage installs binaries to $out/bin/
    # No explicit installPhase needed if that's sufficient.

    meta = with pkgs.lib; {
      description = "Auto scaler for Chromium builds";
    };
  };

# 2. Build the Docker image
# This uses dockerTools.buildImage to create a container image.
  dockerImageDerivation = pkgs.dockerTools.buildImage {
    name = appName;
    tag = appVersion;

    # Define the layers to be copied to the root of the image.
    # We'll copy the compiled binary into /app/ within the image.
    copyToRoot = [
      (pkgs.runCommand "${appName}-binary-layer" {} ''
        mkdir -p $out/app
        cp ${rustApp}/bin/${appName} $out/app/${appName}
        chmod +x $out/app/${appName} # Ensure the binary is executable
      '')
      pkgs.cacert # Include CA certificates for HTTPS requests from the app
    ];

    # Configure the Docker image metadata.
    config = {
      WorkingDir = "/app";
      Cmd = [ "./${appName}" ]; # Command to run when the container starts
      Env = [ "RUST_LOG=info" ];
    };
  };

in
{
  image = dockerImageDerivation; # The actual Docker image derivation
  imageName = appName;           # The application name (from Cargo.toml)
  imageTag = appVersion;         # The application version (from Cargo.toml)
}
