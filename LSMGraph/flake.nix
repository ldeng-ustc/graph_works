{
  description = "a high-performance dynamic graph storage system with multi-level csr";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/24.11";
    utils.url = "github:numtide/flake-utils";
  };

  outputs = {
    self,
    nixpkgs,
    ...
  } @ inputs:
    inputs.utils.lib.eachSystem [
      "aarch64-linux"
      "aarch64-darwin"
      "x86_64-linux"
      "x86_64-darwin"
    ] (system: let
      pkgs = import nixpkgs {
        inherit system;
      };
    in {
      devShells.default = pkgs.mkShell {
        packages = with pkgs; [
          llvmPackages_18.clang
          llvmPackages_18.libclang
          llvmPackages_18.openmp
          backward-cpp
          cmake
          fmt
          gflags
          gperftools
          gdb
          gtest
          spdlog
          tbb
          pkg-config
        ];
        shellHook = ''
          export OMP_INCLUDE_PATH=${pkgs.llvmPackages_18.openmp.dev}/include
          export OMP_LIB_PATH=${pkgs.llvmPackages_18.openmp}/lib
          export PYTHONPATH=$(find ${pkgs.gcc.cc.lib}/share -name python -type d | head -n1):$PYTHONPATH
          echo "GDB pretty printers path: $(find ${pkgs.gcc.cc.lib}/share -name python -type d | head -n1)"
        '';
      };
    });
}
