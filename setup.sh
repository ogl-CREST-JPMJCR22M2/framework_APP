#!/bin/bash

rm /tmp/block_store/*

rm -rf ~/framework

git clone https://github.com/ogl-CREST-JPMJCR22M2/framework.git ~/ --depth=1 

cd ~/framework

./vcpkg/build_iroha_deps.sh $PWD/vcpkg-build

cmake -B build -DCMAKE_TOOLCHAIN_FILE=$PWD/vcpkg-build/scripts/buildsystems/vcpkg.cmake . -DCMAKE_BUILD_TYPE=RELEASE   -GNinja -DUSE_BURROW=OFF -DUSE_URSA=OFF -DTESTING=OFF -DPACKAGE_DEB=OFF

cmake --build ./build --target irohad

